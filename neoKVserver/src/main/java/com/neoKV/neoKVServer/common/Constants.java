package com.neoKV.neoKVServer.common;

/**
 * @author neo82
 */
public class Constants {
    public static final String USER_HOME = System.getProperty("user.home");
    public static final String DATA_FILE_DIR = USER_HOME + "/.neoKV/data/";
    public static final String INDEX_FILE_DIR = USER_HOME + "/.neoKV/index/";

    public static final String META_FILE_DIR = USER_HOME + "/.neoKV/meta/";
    public static final String DATA_FILE_NAME_FORMAT = "data_%d.db";
    public static final String INDEX_FILE_NAME_FORMAT = "index_%d.db";

    public static final String META_FILE_NAME = "meta.json";
}
