package com.neoKV.network.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public enum FileOrderBy {
    CREATION_TIME {
        @Override
        public Number getOrderKey(Path path) throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            return attributes.creationTime().toMillis();
        }
    },
    MODIFICATION_TIME {
        @Override
        public Number getOrderKey(Path path) throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            return attributes.lastModifiedTime().toMillis();
        }
    };




    public abstract Number getOrderKey(Path path) throws IOException;
}
