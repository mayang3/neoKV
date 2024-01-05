package com.neoKV.neoKVServer.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * @author neo82
 */
public class DirectBufferReader {

    private static final DirectBufferReader instance = new DirectBufferReader();

    public static DirectBufferReader getInstance() {
        return instance;
    }

    public ByteBuffer read(Path path) throws IOException {
        ByteBuffer buffer;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            buffer = ByteBuffer.allocateDirect((int) channel.size());
            channel.read(buffer);
            buffer.flip();
        }

        return buffer;
    }

    public ByteBuffer findPos(Path dataFilePath, String key, int lower, int upper) throws IOException {
        ByteBuffer byteBuffer = this.read(dataFilePath);
        byteBuffer.position(lower);

        while (byteBuffer.hasRemaining() && byteBuffer.position() <= upper) {
            int curPos = byteBuffer.position();
            int totalLength = byteBuffer.getInt();
            byte tombstone = byteBuffer.get();

            if (tombstone == (byte) 0) {
                int keyLength = byteBuffer.getInt();
                byte[] bytes = new byte[keyLength];
                byteBuffer.get(bytes);

                if (Arrays.equals(bytes, key.getBytes(StandardCharsets.UTF_8))) {
                    return byteBuffer;
                }

                byteBuffer.position(curPos + 4 + totalLength);
            }
        }

        return null;
    }
}
