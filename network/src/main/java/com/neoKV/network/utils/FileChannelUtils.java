package com.neoKV.network.utils;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author neo82
 */
public class FileChannelUtils {
    private FileChannelUtils() {}


    public static void closeQuietly(FileChannel fileChannel) {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException e) {
            }
        }
    }
}
