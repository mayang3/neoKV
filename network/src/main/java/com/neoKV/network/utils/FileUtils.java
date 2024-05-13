package com.neoKV.network.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author neo82
 */
public class FileUtils {
    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {}


    public static void closeQuietly(FileChannel fileChannel) {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException e) {
            }
        }
    }


    public static void deleteAll(Path dir, String pattern) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, pattern)) {
            for (Path file : stream) {
                Files.deleteIfExists(file);
            }
        } catch (Exception e) {
            log.error("[FileUtils] deleteAll error! dir:{}, pattern:{}", dir, pattern, e);
        }
    }

    public static void changeExtension(Path dir, String oldExtension, String newExtension) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + oldExtension)) {
            for (Path file : stream) {
                String fileName = file.toString();
                String newFileName = fileName.replaceAll(oldExtension + "$", newExtension);
                Path newFile = Paths.get(newFileName);
                Files.move(file, newFile);
            }
        } catch (Exception e) {
            log.error("[FileUtils] changeExtension error! dir:{}, oldExtension:{}, newExtension:{}", dir, oldExtension, newExtension, e);
        }
    }
}
