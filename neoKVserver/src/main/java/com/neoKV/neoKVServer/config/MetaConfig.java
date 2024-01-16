package com.neoKV.neoKVServer.config;

/**
 * @author neo82
 */
public class MetaConfig {
    private int blocNum;

    public MetaConfig(int blocNum) {
        this.blocNum = blocNum;
    }

    public MetaConfig() {
    }

    public static MetaConfig def() {
        return new MetaConfig(1);
    }

    public int getBlocNum() {
        return blocNum;
    }

    public void increaseBlocNum() {
        blocNum++;
    }
}
