package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.merge_compaction.lock.CompactionReadWriteLock;
import com.neoKV.network.common.Constants;
import com.neoKV.network.file.FileOrderBy;
import com.neoKV.network.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author neo82
 */
public class SSTableGroup {
    private static final Logger log = LoggerFactory.getLogger(SSTableGroup.class);
    private static final SSTableGroup instance = new SSTableGroup();

    private final Semaphore semaphore = new Semaphore(1);

    private final TreeMap<Integer, LinkedList<SSTable>> ssTableMap = new TreeMap<>();


    @SuppressWarnings("FieldCanBeLocal")
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, r -> new Thread(r, "SSTableGroup Snapshot Executor"));

    private SSTableGroup() {
        scheduledExecutorService.schedule(this::saveToSSTable, 1, TimeUnit.MINUTES);
    }

    public static SSTableGroup getInstance() {
        return instance;
    }

    public void saveToSSTable() {
        try {
            semaphore.acquire();
            CompactionReadWriteLock.writeLock().lock();

            String uuid = UUID.randomUUID().toString();

            MemtableSnapshot memtableSnapshot = Memtable.getInstance().snapshot();

            if (memtableSnapshot.isEmpty()) {
                return;
            }

            Path dataFilePath = Paths.get(FilePathUtils.getDataFilePath(0, uuid));
            Path indexFilePath = Paths.get(FilePathUtils.getIndexFilePath(0, uuid));

            ssTableMap.computeIfAbsent(0, t -> new LinkedList<>()).addFirst(new SSTable(dataFilePath, indexFilePath, memtableSnapshot.entrySet()));

        } catch (Exception e) {
            log.error("[SSTableGroup] saveToSSTable error!", e);
        } finally {
            CompactionReadWriteLock.writeLock().unlock();
            semaphore.release();
        }
    }

    public ByteBuffer get(String key) {
        for (LinkedList<SSTable> ssTableList : ssTableMap.values()) {
            for (SSTable ssTable : ssTableList) {
                if (ssTable.mightContains(key)) {
                    ByteBuffer byteBuffer = ssTable.get(key);
                    if (byteBuffer != null && byteBuffer.hasRemaining()) {
                        return byteBuffer;
                    }
                }
            }
        }

        return null;
    }

    public void loadSSTableGroup() {
        for (int level : NeoKVServerConfig.getConfig().allLevels()) {
            try {
                for (Path dataFilePath : FilePathUtils.getPathListOrderBy(String.format(Constants.DATA_FILE_DIR, level), FileOrderBy.CREATION_TIME)) {

                    if (!Files.exists(dataFilePath) || !Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
                        log.error("[SSTableGroup] not found dataPath:{}", dataFilePath);
                        continue;
                    }

                    ssTableMap.computeIfAbsent(level, t -> new LinkedList<>()).addFirst(new SSTable(dataFilePath));
                }
            } catch (Exception e) {
                log.error("[SSTableGroup] loadSSTableGroup error!", e);
            }
        }
    }
}
