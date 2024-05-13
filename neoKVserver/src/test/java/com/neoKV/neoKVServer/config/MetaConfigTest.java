package com.neoKV.neoKVServer.config;

import com.neoKV.network.utils.YamlUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetaConfigTest {

    @Test
    public void metaConfig_level() {
        MetaConfig metaConfig = YamlUtils.readValue("neokv-config.yml", MetaConfig.class);

        Assertions.assertEquals(metaConfig.getLevel().get(0), 10);
        Assertions.assertEquals(metaConfig.getLevel().get(1), 100);
        Assertions.assertEquals(metaConfig.getLevel().get(2), 1000);
        Assertions.assertEquals(metaConfig.getLevel().get(3), 10000);
    }

    @Test
    public void metaConfig_range() {
        MetaConfig metaConfig = YamlUtils.readValue("neokv-config.yml", MetaConfig.class);

        Assertions.assertEquals(metaConfig.getTableRangeList().size(), 7);
    }
}