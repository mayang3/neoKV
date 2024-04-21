package com.neoKV.neoKVServer.file;

import com.neoKV.network.common.Constants;

import java.io.FileInputStream;
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

    public ByteBuffer read(Path path, int lower, int upper) throws IOException {
        ByteBuffer buffer;
        try (FileInputStream fis = new FileInputStream(path.toFile()); FileChannel fileChannel = fis.getChannel()) {
            buffer = ByteBuffer.allocateDirect(upper - lower + 1);

            fileChannel.position(lower);
            fileChannel.read(buffer);
        }

        return buffer;
    }

    public ByteBuffer findPos(Path dataFilePath, String key, int lower, int upper) throws IOException {
        ByteBuffer byteBuffer = this.read(dataFilePath, lower, upper);
//        byteBuffer.position(lower);

        while (byteBuffer.hasRemaining() && byteBuffer.position() <= upper) {
            int curPos = byteBuffer.position();
            int totalLength = byteBuffer.getInt();

            // read key bytes
            int keyLength = byteBuffer.getInt();
            byte[] bytes = new byte[keyLength];
            byteBuffer.get(bytes);

            if (Arrays.equals(bytes, key.getBytes(StandardCharsets.UTF_8))) {
                byte[] body = new byte[totalLength - Constants.KEY_SIZE_BYTE_LENGTH - keyLength];
                byteBuffer.get(body, 0, body.length);

                return ByteBuffer.wrap(body); // tombstone + timestamp + data type + data bytes
            }

            byteBuffer.position(curPos + Constants.TOTAL_SIZE_BYTE_LENGTH + totalLength);
        }

        return null;
    }
}
