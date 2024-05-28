package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.network.common.Constants;
import com.neoKV.network.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author neo82
 */
public class SSTableTemp {
    private static final Logger log = LoggerFactory.getLogger(SSTableTemp.class);
    private final Path dataFilePath;
    private final Path indexFilePath;
    private final AtomicInteger dataCount = new AtomicInteger(0);


    public SSTableTemp(Path dataFilePath) {
        this.dataFilePath = dataFilePath;
        this.indexFilePath = FilePathUtils.getIndexFilePathBy(dataFilePath.toString());
    }

    public void commit(DataRecord dataRecord) {
        Long pos = DirectBufferWriter.getInstance().saveToFileQuietly(dataFilePath, dataRecord.toDirectByteBuffer());

        if (pos != null) {
            String key = new String(dataRecord.getKey());

            if (dataCount.getAndIncrement() % Constants.SPARSE_INDEX_DENSITY == 0) {
                appendIndexToFile(dataRecord, key, pos);
            }
        }

        log.debug("[SSTableTemp] commit dataRecord:{}", dataRecord);
    }

    private void appendIndexToFile(DataRecord dataRecord, String key, Long pos) {
        ByteBuffer indexBuffer = ByteBuffer.allocateDirect(Constants.KEY_SIZE_BYTE_LENGTH + key.length() + Constants.INDEX_POSITION_SIZE_BYTE_LENGTH);

        indexBuffer.putInt(key.length());
        indexBuffer.put(dataRecord.getKey());
        indexBuffer.putLong(pos);

        indexBuffer.flip();

        DirectBufferWriter.getInstance().saveToFileQuietly(indexFilePath, indexBuffer);
    }
}
