package com.neoKV.neoKVServer.merge_compaction.compaction;

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
        String key = "KEY";
        byte tombstone = Constants.TOMBSTONE_ALIVE;
        long timestamp = System.currentTimeMillis();
        byte dataType = DataType.INTEGER.getCode();
        int value = 123123;

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

        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        fileChannel.write(buffer);
    }

}