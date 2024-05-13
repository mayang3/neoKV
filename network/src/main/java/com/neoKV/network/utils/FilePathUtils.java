package com.neoKV.network.utils;

import com.neoKV.network.common.Constants;
import com.neoKV.network.exception.NeoKVException;
import com.neoKV.network.file.FileOrderBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author neo82
 */
public class FilePathUtils {
    private static final Logger log = LoggerFactory.getLogger(FilePathUtils.class);

    private FilePathUtils() {
    }

    public static Collection<Path> getIndexPathListOrderBy(int level, FileOrderBy fileOrderBy) {
        return getPathListOrderBy(String.format(Constants.INDEX_FILE_DIR, level), fileOrderBy);
    }

    public static Collection<Path> getPathListOrderBy(String dir, FileOrderBy fileOrderBy) {
        TreeMap<? super Number, Path> fileMap = new TreeMap<>();

        Path dirPath = Paths.get(dir);

        if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
            try (Stream<Path> pathStream = Files.list(dirPath)) {

                pathStream.filter(Files::isRegularFile)
                          .forEach(path -> {
                              try {
                                  fileMap.put(fileOrderBy.getOrderKey(path), path);
                              } catch (IOException e) {
                                  throw new NeoKVException(String.format("path : %s", path), e);
                              }
                          });

            } catch (Exception e) {
                log.error("[FilePathUtils] getPathListOrderBy error! dir:{}, fileOrderBy:{}", dir, fileOrderBy, e);
            }
        }

        return fileMap.values();
    }

    public static String getDataFilePath(int level, String uuid) {
        return String.format(Constants.DATA_FILE_DIR, level) + String.format(Constants.DATA_FILE_NAME_FORMAT, uuid);
    }

    public static String getMergeDataFilePath(int level, String uuid) {
        return String.format(Constants.DATA_FILE_DIR, level) + String.format(Constants.DATA_FILE_MERGE_NAME_FORMAT, uuid);
    }

    public static Path getDataFileDir(int level) {
        return Path.of(String.format(Constants.DATA_FILE_DIR, level));
    }

    public static Path getDataFilePathBy(String indexPath) {
        String[] parts = indexPath.split("/");

        String[] lastStrings = parts[parts.length - 1].split("_");
        lastStrings[0] = Constants.DATA_FILE_PREFIX;
        parts[parts.length - 1] = String.join("_", lastStrings);

        parts[parts.length - 3] = Constants.DATA_FILE_PREFIX;


        return Path.of(String.join("/", parts));
    }

    public static Path getIndexFilePathBy(String dataPath) {
        String[] parts = dataPath.split("/");

        String[] lastStrings = parts[parts.length - 1].split("_");
        lastStrings[0] = Constants.INDEX_FILE_PREFIX;
        parts[parts.length - 1] = String.join("_", lastStrings);

        parts[parts.length - 3] = Constants.INDEX_FILE_PREFIX;


        return Path.of(String.join("/", parts));
    }

    public static String getIndexFilePath(int level, String uuid) {
        return String.format(Constants.INDEX_FILE_DIR, level) + String.format(Constants.INDEX_FILE_NAME_FORMAT, uuid);
    }
}
