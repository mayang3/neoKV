package com.neoKV.neoKVServer.merge_compaction.iterator;

import com.neoKV.neoKVServer.storage.DataRecord;
import com.neoKV.network.common.Constants;
import com.neoKV.network.exception.NeoKVException;
import com.neoKV.network.utils.ByteBufferUtils;
import com.neoKV.network.utils.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author neo82
 */
public class MergeIterator implements Iterable<ByteBuffer>, AutoCloseable {

    private FileChannel fileChannel = null;
    private ByteBuffer headBuffer = null;
    private ByteBuffer bodyBuffer = null;
    private Iter iter = null;

    public MergeIterator(Path path)  {
        try {
            this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            this.headBuffer = ByteBuffer.allocateDirect(Constants.TOTAL_SIZE_BYTE_LENGTH);
        } catch (Exception e) {
            FileUtils.closeQuietly(this.fileChannel);
            ByteBufferUtils.clean(this.headBuffer);
            ByteBufferUtils.clean(this.bodyBuffer);
            throw new NeoKVException(e);
        }
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        if (this.iter == null) {
            this.iter = new Iter();
        }

        return this.iter;
    }

    public DataRecord getOneRecord() {
        Iterator<ByteBuffer> it = this.iterator();

        if (it.hasNext()) {
            return DataRecord.of(it.next());
        }

        return DataRecord.empty();
    }

    @Override
    public void close() {
        FileUtils.closeQuietly(this.fileChannel);
        ByteBufferUtils.clean(this.headBuffer);
        ByteBufferUtils.clean(this.bodyBuffer);
    }

    private class Iter implements Iterator<ByteBuffer> {
        @Override
        public boolean hasNext() {
            try {
                return fileChannel.position() < fileChannel.size();
            } catch (IOException e) {
                throw new NeoKVException("Error checking next element availability", e);
            }
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No More Elements to read");
            }

            try {
                headBuffer.clear();
                fileChannel.read(headBuffer);
                headBuffer.flip();

                int bodySize = headBuffer.getInt();
                if (bodyBuffer == null || bodyBuffer.capacity() < bodySize) {
                    ByteBufferUtils.clean(bodyBuffer);
                    bodyBuffer = ByteBuffer.allocateDirect(bodySize);
                } else {
                    bodyBuffer.clear();
                    bodyBuffer.limit(bodySize);
                }

                fileChannel.read(bodyBuffer);
                bodyBuffer.flip();
                return bodyBuffer;
            } catch (IOException e) {
                throw new NeoKVException("Failed to read next element", e);
            }
        }
    }
}
