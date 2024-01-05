package com.neoKV.neoKVServer.file;

import com.neoKV.neoKVServer.filter.SparseIndex;
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
    private static final DirectBufferWriter instance = new DirectBufferWriter();
    public static DirectBufferWriter getInstance() {
        return instance;
    }

    public SparseIndex writeData(Path path, Set<Map.Entry<String, byte[]>> data, int totalLength) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(totalLength);
        int i = 0;

        SparseIndex sparseIndex = new SparseIndex();

        Iterator<Map.Entry<String, byte[]>> iterator = data.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, byte[]> entry = iterator.next();

            if (i++ % sparseIndex.getDensity() == 0 || !iterator.hasNext()) {
                sparseIndex.put(entry.getKey(), buffer.position());
            }

            byte [] keyBytes = entry.getKey().getBytes();

            buffer.putInt(1 + 4 + keyBytes.length + entry.getValue().length);
            buffer.put((byte)0);
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.put(entry.getValue()); // pure data
        }

        buffer.flip();
        saveToFile(path, buffer);

        return sparseIndex;
    }

    public void writeIndex(Path indexPath, SparseIndex sparseIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(sparseIndex.getTotalSize());

        for (Map.Entry<String, Integer> entry : sparseIndex.getIndices().entrySet()) {
            byte [] bytes = entry.getKey().getBytes();
            buffer.putInt(bytes.length);
            buffer.put(bytes);
            buffer.putInt(entry.getValue());
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
