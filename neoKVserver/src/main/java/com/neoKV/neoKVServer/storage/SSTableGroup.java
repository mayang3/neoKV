package com.neoKV.neoKVServer.storage;

import com.neoKV.network.common.Constants;
import com.neoKV.neoKVServer.config.MetaConfig;
import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.file.DirectBufferReader;
import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author neo82
 */
public class SSTableGroup {
    private static final Logger log = LoggerFactory.getLogger(SSTableGroup.class);
    private static final SSTableGroup instance = new SSTableGroup();

    private final List<SSTableCore> ssTableList = new LinkedList<>();


    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, r -> new Thread(r, "[SSTableGroup Snapshot Executor]"));

    private SSTableGroup() {
        scheduledExecutorService.schedule(this::saveToSSTable, 1, TimeUnit.MINUTES);
    }

    public static SSTableGroup getInstance() {
        return instance;
    }

    public void saveToSSTable() {
        MetaConfig metaConfig = NeoKVServerConfig.getInstance().getMetaConfig();

        try {
            int num = metaConfig.getBlocNum();

            MemtableSnapshot memtableSnapshot = Memtable.getInstance().snapshot();

            Path dataPath = Paths.get(Constants.DATA_FILE_DIR + String.format(Constants.DATA_FILE_NAME_FORMAT, num));
            Path indexPath = Paths.get(Constants.INDEX_FILE_DIR + String.format(Constants.INDEX_FILE_NAME_FORMAT, num));

            SparseIndex sparseIndex = saveData(dataPath, memtableSnapshot.entrySet());
            saveIndex(indexPath, sparseIndex);

            loadSSTable(memtableSnapshot.keySet(), sparseIndex, dataPath, indexPath);
        } catch (Exception e) {
            log.error("[SSTableGroup] saveToSSTable error!", e);
        } finally {
            NeoKVServerConfig.getInstance().incrementAndWrite();
        }
    }

    private void saveIndex(Path indexPath, SparseIndex sparseIndex) throws IOException {
        DirectBufferWriter.getInstance().writeIndex(indexPath, sparseIndex);
    }

    private SparseIndex saveData(Path dataPath, Set<Map.Entry<String, byte[]>> data) throws IOException {
        return DirectBufferWriter.getInstance().writeData(dataPath, data);
    }

    public ByteBuffer get(String key) {
        for (SSTableCore ssTable : ssTableList) {
            if (ssTable.mightContains(key)) {
                ByteBuffer byteBuffer = ssTable.get(key);
                if (byteBuffer != null && byteBuffer.hasRemaining()) {
                    return byteBuffer;
                }
            }
        }

        return null;
    }

    public void loadSSTableGroup() {
        MetaConfig metaConfig = NeoKVServerConfig.getInstance().getMetaConfig();

        for (int i = 1; i <= metaConfig.getBlocNum(); i++) {
            try {
                Path dataPath = Paths.get(Constants.DATA_FILE_DIR + String.format(Constants.DATA_FILE_NAME_FORMAT, i));
                Path indexPath = Paths.get(Constants.INDEX_FILE_DIR + String.format(Constants.INDEX_FILE_NAME_FORMAT, i));

                if (!Files.exists(dataPath) || !Files.exists(indexPath) || Files.size(dataPath) == 0 || Files.size(indexPath) == 0) {
                    log.error("[SSTableGroup] not found dataPath:{} or indexPath:{}", dataPath, indexPath);
                    continue;
                }

                SparseIndex sparseIndex = readSparseIndex(indexPath);

                loadSSTable(sparseIndex.getIndices().keySet(), sparseIndex, dataPath, indexPath);
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

    private void loadSSTable(Set<String> keys, SparseIndex sparseIndex, Path dataPath, Path indexPath) {
        SSTableCore ssTable = new SSTableCore(keys, sparseIndex, dataPath, indexPath);

        ((LinkedList<SSTableCore>) this.ssTableList).addFirst(ssTable);
    }
}
