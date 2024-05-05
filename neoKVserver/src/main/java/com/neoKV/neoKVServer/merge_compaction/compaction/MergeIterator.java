package com.neoKV.neoKVServer.merge_compaction.compaction;

import com.neoKV.network.common.Constants;
import com.neoKV.network.exception.NeoKVException;
import com.neoKV.network.utils.ByteBufferUtils;
import com.neoKV.network.utils.FileChannelUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * @author neo82
 */
public class MergeIterator implements Iterable<ByteBuffer>, AutoCloseable{

    private final FileChannel fileChannel;
    private final ByteBuffer headBuffer;
    private ByteBuffer bodyBuffer;

    public MergeIterator(Path path) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.headBuffer = ByteBuffer.allocateDirect(Constants.TOTAL_SIZE_BYTE_LENGTH);
        this.bodyBuffer = null;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new Iter();
    }

    @Override
    public void close() throws IOException {
        FileChannelUtils.closeQuietly(this.fileChannel);
        ByteBufferUtils.clean(this.headBuffer);
        ByteBufferUtils.clean(this.bodyBuffer);
    }

    private class Iter implements Iterator<ByteBuffer> {
        @Override
        public boolean hasNext() {
            try {
                return fileChannel.position() < fileChannel.size();
            } catch (IOException e) {
                throw new NeoKVException(e);
            }
        }

        @Override
        public ByteBuffer next() {
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
                throw new NeoKVException(e);
            }
        }
    }
}
