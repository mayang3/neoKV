package com.neoKV.network.utils;

import com.neoKV.network.common.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

public class FilePathUtilsTest {

    @Test
    public void testDataFilePathBy() {
        String uuid = "9b0d6cdc-4a6f-4178-8d5a-7267634c3b07";
        String indexFilePath = String.format(Constants.INDEX_FILE_DIR, 1) + String.format(Constants.INDEX_FILE_NAME_FORMAT, uuid);

        Path path = FilePathUtils.getDataFilePathBy(indexFilePath);

        Assertions.assertEquals(String.format(Constants.DATA_FILE_DIR, 1) + String.format(Constants.DATA_FILE_NAME_FORMAT, uuid), path.toString());
    }
}