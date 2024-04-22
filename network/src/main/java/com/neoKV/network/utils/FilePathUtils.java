package com.neoKV.network.utils;

import com.neoKV.network.common.Constants;

/**
 * @author neo82
 */
public class FilePathUtils {

    private FilePathUtils() {}

    public static String getDataFilePath(int num) {
        return Constants.DATA_FILE_DIR + String.format(Constants.DATA_FILE_NAME_FORMAT, num);
    }

    public static String getIndexFilePath(int num) {
        return Constants.INDEX_FILE_DIR + String.format(Constants.INDEX_FILE_NAME_FORMAT, num);
    }
}
