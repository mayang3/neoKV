package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.file.DirectBufferReader;
import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import com.neoKV.network.file.FileOrderBy;
import com.neoKV.network.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author neo82
 */
public class SSTableGroup {
    private static final Logger log = LoggerFactory.getLogger(SSTableGroup.class);
    private static final SSTableGroup instance = new SSTableGroup();

    private final TreeMap<Integer, LinkedList<SSTable>> ssTableMap = new TreeMap<>();


    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, r -> new Thread(r, "SSTableGroup Snapshot Executor"));

    private SSTableGroup() {
        scheduledExecutorService.schedule(this::saveToSSTable, 1, TimeUnit.MINUTES);
    }

    public static SSTableGroup getInstance() {
        return instance;
    }

    public void saveToSSTable() {
        try {
            String uuid = UUID.randomUUID().toString();

            MemtableSnapshot memtableSnapshot = Memtable.getInstance().snapshot();

            if (memtableSnapshot.isEmpty()) {
                return;
            }

            Path dataPath = Paths.get(FilePathUtils.getDataFilePath(0, uuid));
            Path indexPath = Paths.get(FilePathUtils.getIndexFilePath(0, uuid));

            SparseIndex sparseIndex = saveData(dataPath, memtableSnapshot.entrySet());
            saveIndex(indexPath, sparseIndex);

            loadSSTable(0, memtableSnapshot.keySet(), sparseIndex, dataPath, indexPath);
        } catch (Exception e) {
            log.error("[SSTableGroup] saveToSSTable error!", e);
        }
    }

    private void saveIndex(Path indexPath, SparseIndex sparseIndex) throws IOException {
        DirectBufferWriter.getInstance().writeIndex(indexPath, sparseIndex);
    }

    private SparseIndex saveData(Path dataPath, Set<Map.Entry<String, byte[]>> data) throws IOException {
        return DirectBufferWriter.getInstance().writeData(dataPath, data);
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
                for (Path indexFilePath : FilePathUtils.getIndexPathListOrderBy(level, FileOrderBy.CREATION_TIME)) {

                    Path dataFilePath = FilePathUtils.getDataFilePathBy(indexFilePath.toString());

                    if (!Files.exists(dataFilePath) || !Files.exists(indexFilePath) || Files.size(dataFilePath) == 0 || Files.size(indexFilePath) == 0) {
                        log.error("[SSTableGroup] not found dataPath:{} or indexPath:{}", dataFilePath, indexFilePath);
                        continue;
                    }

                    SparseIndex sparseIndex = readSparseIndex(indexFilePath);

                    loadSSTable(level, sparseIndex.getIndices().keySet(), sparseIndex, dataFilePath, indexFilePath);
                }
            } catch (Exception e) {
                log.error("[SSTableGroup] loadSSTableGroup error!", e);
            }
        }
    }

    private SparseIndex readSparseIndex(Path indexPath) throws IOException {
        SparseIndex sparseIndex = new SparseIndex();

        ByteBuffer byteBuffer = DirectBufferReader.getInstance().read(indexPath);

        while (byteBuffer.hasRemaining()) {
            int keyLength = byteBuffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            byteBuffer.get(keyBytes, 0, keyLength);

            sparseIndex.put(new String(keyBytes), byteBuffer.getLong());
        }

        return sparseIndex;
    }

    private void loadSSTable(int level, Set<String> keys, SparseIndex sparseIndex, Path dataPath, Path indexPath) {
        SSTable ssTable = new SSTable(keys, sparseIndex, dataPath, indexPath);

        ssTableMap.computeIfAbsent(level, t -> new LinkedList<>()).addFirst(ssTable);
    }
}
