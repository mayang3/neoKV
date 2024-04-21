package com.neoKV.network.common;

/**
 * @author neo82
 */
public class Constants {

    public static final int TOTAL_SIZE_BYTE_LENGTH = 4;
    public static final int TOMBSTONE_BYTE_LENGTH = 1;

    public static final int ADMIN_COMMAND_BYTE_LENGTH = 4;

    public static final int TIMESTAMP_BYTE_LENGTH = 8;

    public static final byte TOMBSTONE_ALIVE = 0;

    public static final byte TOMBSTONE_DELETED = 1;

    public static final int DATATYPE_BYTE_LENGTH = 1;

    public static final int KEY_SIZE_BYTE_LENGTH = 4;

    public static final int INDEX_KEY_SIZE_BYTE_LENGTH = 4;

    public static final int INDEX_POSITION_SIZE_BYTE_LENGTH = 8;

    public static final String USER_HOME = System.getProperty("user.home");
    public static final String DATA_FILE_DIR = USER_HOME + "/.neoKV/data/";
    public static final String INDEX_FILE_DIR = USER_HOME + "/.neoKV/index/";

    public static final String META_FILE_DIR = USER_HOME + "/.neoKV/meta/";
    public static final String DATA_FILE_NAME_FORMAT = "data_%d.db";
    public static final String INDEX_FILE_NAME_FORMAT = "index_%d.db";

    public static final String MERGE_AND_COMPACTION_POSTFIX = ".merge";

    public static final String META_FILE_NAME = "meta.json";
    public static final int SPARSE_INDEX_DENSITY = 2;
}
