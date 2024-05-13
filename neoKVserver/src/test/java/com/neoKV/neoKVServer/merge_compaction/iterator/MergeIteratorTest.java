package com.neoKV.neoKVServer.merge_compaction.iterator;

import com.neoKV.neoKVServer.protocol.DataProtocol;
import com.neoKV.network.DataType;
import com.neoKV.network.common.Constants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class MergeIteratorTest {


    @Test
    public void mergeIterator() throws IOException {
        Path path = Path.of("tempFile.db");

        try {
            writeFile(path);

            try (MergeIterator iterator = new MergeIterator(path)) {
                for (ByteBuffer buffer : iterator) {
                    int keyLength = buffer.getInt();
                    byte[] keyBytes = new byte[keyLength];

                    buffer.get(keyBytes);

                    byte tombstone = buffer.get();
                    long timestamp = buffer.getLong();
                    byte dataType = buffer.get();
                    int value = buffer.getInt();


                    System.out.println("========================");

                    System.out.println(keyLength);
                    System.out.println(new String(keyBytes));
                    System.out.println(tombstone);
                    System.out.println(timestamp);
                    System.out.println(DataType.of(dataType));
                    System.out.println(value);

                    System.out.println("========================");
                }
            }


        } finally {
            removeFile(path);
        }
    }

    private void removeFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
        }
    }

    private static void writeFile(Path path) throws IOException {
        for (int i = 1; i <= 5; i++) {
            String key = "KEY" + i;
            byte tombstone = Constants.TOMBSTONE_ALIVE;
            long timestamp = System.currentTimeMillis();
            byte dataType = DataType.INTEGER.getCode();
            int value = 2 * i;

            int bodySize = 2 + 8 + 4;

            ByteBuffer body = ByteBuffer.allocate(bodySize);
            body.put(tombstone);
            body.putLong(timestamp);
            body.put(dataType);
            body.putInt(value);
            body.flip();

            int totalLength = 4 + 4 + key.length() + bodySize;

            ByteBuffer buffer = ByteBuffer.allocate(totalLength);
            DataProtocol.getInstance().putData(buffer, key.getBytes(), body.array());
            buffer.flip();

            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            fileChannel.write(buffer);
        }
    }

}