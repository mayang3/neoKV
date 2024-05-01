package com.neoKV.neoKVServer.config;

import com.neoKV.network.utils.YamlUtils;

/**
 * @author neo82
 */
public class NeoKVServerConfig {
    private final MetaConfig metaConfig;

    private static final NeoKVServerConfig INSTANCE = new NeoKVServerConfig();

    private static final String CONFIG_FILE_NAME = "neokv-config.yml";

    public static MetaConfig getConfig() {
        return INSTANCE.metaConfig;
    }

    private NeoKVServerConfig() {
        this.metaConfig = YamlUtils.readValue(CONFIG_FILE_NAME, MetaConfig.class);
    }
}
