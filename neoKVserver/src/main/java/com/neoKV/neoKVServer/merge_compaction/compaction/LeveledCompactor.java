package com.neoKV.neoKVServer.merge_compaction.compaction;

import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.merge_compaction.iterator.MergeIterator;
import com.neoKV.neoKVServer.merge_compaction.lock.CompactionReadWriteLock;
import com.neoKV.neoKVServer.storage.DataRecord;
import com.neoKV.neoKVServer.storage.SSTable;
import com.neoKV.network.exception.NeoKVException;
import com.neoKV.network.utils.FilePathUtils;
import com.neoKV.network.utils.FileUtils;
import com.neoKV.network.utils.Range;
import com.neoKV.network.utils.RangeTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LeveledCompactor {
    private static final Logger log = LoggerFactory.getLogger(LeveledCompactor.class);

    private static final LeveledCompactor INSTANCE = new LeveledCompactor();

    public static LeveledCompactor getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("FieldCanBeLocal")
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, r -> new Thread(r, "LeveledCompactor Executor"));

    private LeveledCompactor() {
        scheduledExecutorService.scheduleWithFixedDelay(this::mergeAndCompact, 5000, 2000, TimeUnit.MILLISECONDS);
    }

    public void mergeAndCompact() {
        log.info("[LeveledCompactor] start!");

        List<MergeIterator> mergeIteratorList = new ArrayList<>();

        try {
            int maxLevel = NeoKVServerConfig.getConfig().getMaxLevel();

            for (int level = 0; level < maxLevel; level++) {
                int nextLevel = level + 1;

                addMergeIterator(mergeIteratorList, level);
                addMergeIterator(mergeIteratorList, nextLevel);

                // Initialize PriorityQueue
                PriorityQueue<Item> pq = makePriorityQueue();

                int mask = (1 << mergeIteratorList.size()) - 1;

                for (int i = 0; i < mergeIteratorList.size(); i++) {
                    DataRecord record = mergeIteratorList.get(i).getOneRecord();

                    if (record.isEmpty()) {
                        mask &= ~(1 << i);
                    } else {
                        pq.add(new Item(i, record));
                    }
                }

                RangeTree<Integer, SSTable> rangeTree = new RangeTree<>();

                for (Range<Integer> tableRange : NeoKVServerConfig.getConfig().getTableRangeList()) {
                    rangeTree.put(tableRange, new SSTable(Path.of(FilePathUtils.getMergeDataFilePath(nextLevel, UUID.randomUUID().toString()))));
                }

                // merge & compaction
                while (!pq.isEmpty() && mask != 0) {
                    Item first = pq.poll();

                    Set<Integer> indexSet = new HashSet<>();
                    indexSet.add(Objects.requireNonNull(first).index);

                    while (!pq.isEmpty() && Arrays.equals(pq.peek().dataRecord.getKey(), Objects.requireNonNull(first).dataRecord.getKey())) {
                        indexSet.add(Objects.requireNonNull(pq.poll()).index);
                    }

                    String key = new String(first.dataRecord.getKey());

                    SSTable ssTable = rangeTree.get(key.codePointAt(0));
                    ssTable.commit(first.dataRecord);

                    for (int next : indexSet) {
                        DataRecord record = mergeIteratorList.get(next).getOneRecord();

                        if (record.isEmpty()) {
                            mask &= ~(1 << next);
                        } else {
                            pq.add(new Item(next, record));
                        }
                    }
                }

                changeFiles(level, nextLevel);
            }
        } catch (Exception e) {
            log.error("[LeveledCompactor] mergeAndCompact error!", e);
        } finally {
            for (MergeIterator mergeIterator : mergeIteratorList) {
                mergeIterator.close();
            }
        }


        log.info("[LeveledCompactor] end!");
    }

    private static void changeFiles(int level, int nextLevel) {
        try {
            CompactionReadWriteLock.writeLock().lock();

            if (level == 0) {
                FileUtils.deleteAll(FilePathUtils.getDataFileDir(level), "*.db");
            }

            FileUtils.deleteAll(FilePathUtils.getDataFileDir(nextLevel), "*.db");
            FileUtils.changeExtension(FilePathUtils.getDataFileDir(nextLevel), ".merge", ".db");
        } finally {
            CompactionReadWriteLock.writeLock().unlock();
        }
    }

    private static PriorityQueue<Item> makePriorityQueue() {
        return new PriorityQueue<>((e1, e2) -> {
            DataRecord o1 = e1.dataRecord;
            DataRecord o2 = e2.dataRecord;

            int keyComp = Arrays.compare(o1.getKey(), o2.getKey());

            if (keyComp == 0) {
                int tombComp = Byte.compare(o2.getTombstone(), o1.getTombstone());
                return tombComp == 0 ? Long.compare(o2.getTimestamp(), o1.getTimestamp()) : tombComp;
            }

            return keyComp;
        });
    }

    private void addMergeIterator(List<MergeIterator> mergeIteratorList, int level) {
        Path path = FilePathUtils.getDataFileDir(level);

        if (Files.exists(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.db")) {
                stream.forEach(path1 -> mergeIteratorList.add(new MergeIterator(path1)));
            } catch (Exception e) {
                throw new NeoKVException(e);
            }
        }
    }

    private static class Item {
        int index;
        DataRecord dataRecord;

        public Item(int index, DataRecord dataRecord) {
            this.index = index;
            this.dataRecord = dataRecord;
        }
    }
}
