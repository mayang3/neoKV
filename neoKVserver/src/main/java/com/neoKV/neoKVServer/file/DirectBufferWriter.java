package com.neoKV.neoKVServer.file;

import com.neoKV.neoKVServer.filter.SparseIndex;
import com.neoKV.neoKVServer.protocol.DataProtocol;
import com.neoKV.neoKVServer.protocol.IndexProtocol;
import com.neoKV.network.utils.ByteBufferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author neo82
 */
public class DirectBufferWriter {

    private static final Logger log = LoggerFactory.getLogger(DirectBufferWriter.class);

    private final DataProtocol dataProtocol = DataProtocol.getInstance();
    private final IndexProtocol indexProtocol = IndexProtocol.getInstance();
    private static final DirectBufferWriter instance = new DirectBufferWriter();

    public static DirectBufferWriter getInstance() {
        return instance;
    }

    public SparseIndex writeData(Path path, Set<Map.Entry<String, byte[]>> data) throws IOException {
        ByteBuffer buffer = null;
        SparseIndex sparseIndex = new SparseIndex();

        try {
            buffer = dataProtocol.allocateDirect(data);
            int i = 0;

            Iterator<Map.Entry<String, byte[]>> iterator = data.iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, byte[]> entry = iterator.next();

                if (i++ % sparseIndex.getDensity() == 0 || !iterator.hasNext()) {
                    sparseIndex.put(entry.getKey(), (long) buffer.position());
                }

                byte[] keyBytes = entry.getKey().getBytes();

                dataProtocol.putData(buffer, keyBytes, entry.getValue());
            }

            buffer.flip();
            saveToFile(path, buffer);
        } finally {
            ByteBufferUtils.clean(buffer);
        }

        return sparseIndex;
    }

    public void writeIndex(Path indexPath, SparseIndex sparseIndex) throws IOException {
        ByteBuffer buffer = indexProtocol.allocateDirect(sparseIndex);

        for (Map.Entry<String, Long> entry : sparseIndex.getIndices().entrySet()) {
            indexProtocol.putIndexes(buffer, entry.getKey().getBytes(), entry.getValue());
        }

        buffer.flip();
        saveToFile(indexPath, buffer);
    }

    private void saveToFile(Path path, ByteBuffer buffer) throws IOException {
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent());
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            if (channel.write(buffer) == 0) {
                log.error("[DirectBufferHandler] file is not available. path : {}", path);
            }
        }
    }
}
