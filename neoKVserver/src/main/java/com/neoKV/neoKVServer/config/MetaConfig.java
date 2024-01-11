package com.neoKV.neoKVServer.config;

/**
 * @author neo82
 */
public class MetaConfig {
    private int blocNum;

    private MetaConfig(int blocNum) {
        this.blocNum = blocNum;
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
