package com.neoKV.neoKVServer.config;

import com.neoKV.neoKVServer.common.Constants;
import com.neoKV.network.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author neo82
 */
public class NeoKVServerConfig {
    private static final Logger log = LoggerFactory.getLogger(NeoKVServerConfig.class);

    private static final NeoKVServerConfig instance = new NeoKVServerConfig();

    private final ReentrantLock incrementLock = new ReentrantLock();

    private MetaConfig metaConfig;

    public static NeoKVServerConfig getInstance() {
        return instance;
    }


    public MetaConfig getMetaConfig() {
        return metaConfig;
    }

    public void load() {
        try {
            File file = new File(Constants.META_FILE_DIR + Constants.META_FILE_NAME);

            if (!file.exists()) {
                createMetaFile(file, MetaConfig.def());
            }

            this.metaConfig = JsonUtils.readValue(file, MetaConfig.class);

        } catch (Exception e) {
            log.error("[NeoKVServerConfig] load error!", e);
            this.metaConfig = MetaConfig.def();
        }
    }

    public void incrementAndWrite() {
        incrementLock.lock();
        try {
            this.metaConfig.increaseBlocNum();
            this.write();
        } finally {
            incrementLock.unlock();
        }
    }

    public void write() {
        try {
            File file = new File(Constants.META_FILE_DIR + Constants.META_FILE_NAME);

            if (file.exists() && !file.delete()) {
                log.error("[NeoKVServerConfig] An error occurred during deletion.");
                return;
            }

            createMetaFile(file, this.metaConfig);
        } catch (Exception e) {
            log.error("[NeoKVServerConfig] write error!", e);
        }
    }

    void createMetaFile(File file, MetaConfig metaConfig) throws IOException {
        File parentFile = file.getParentFile();

        if (!parentFile.exists() && !parentFile.mkdirs()) {
            log.error("[NeoKVServerConfig] An error occurred during mkdirs. absolutePath : {}", file.getAbsolutePath());
            return;
        }

        try (FileWriter writer = new FileWriter(file)) {
            JsonUtils.writeValue(writer, metaConfig);
        }
    }
}
