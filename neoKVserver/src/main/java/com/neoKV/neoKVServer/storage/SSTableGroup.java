package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.common.Constants;
import com.neoKV.neoKVServer.config.MetaConfig;
import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
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
        scheduledExecutorService.schedule(this::saveToSSTable, 5, TimeUnit.MINUTES);
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
        int totalLength = 0;

        for (Map.Entry<String, byte[]> entry : data) {
            totalLength += 4; // totalLength per entry
            totalLength += 1; // tombstone
            totalLength += 4; // key length
            totalLength += entry.getKey().getBytes().length; // key bytes
            totalLength += entry.getValue().length; // with dataType flag
        }

        return DirectBufferWriter.getInstance().writeData(dataPath, data, totalLength);
    }

    private void loadSSTable(Set<String> keys, SparseIndex sparseIndex, Path dataPath, Path indexPath) {
        SSTableCore ssTable = new SSTableCore(keys, sparseIndex, dataPath, indexPath);

        ((LinkedList<SSTableCore>)this.ssTableList).addFirst(ssTable);
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
}
