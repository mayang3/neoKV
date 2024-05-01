package com.neoKV.neoKVServer.config;

import java.util.Set;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class Level {
    private final TreeMap<Integer, Integer> levelMegaBytesMap = new TreeMap<>();


    public int getMegaBytes(int level) {
        return this.levelMegaBytesMap.get(level);
    }

    public int getMaxLevel() {
        return this.levelMegaBytesMap.lastKey();
    }

    public int getMinLevel() {
        return this.levelMegaBytesMap.firstKey();
    }

    public Set<Integer> allLevels() {
        return levelMegaBytesMap.keySet();
    }

    @Override
    public String toString() {
        return "Level{" +
                "levelMegaBytesMap=" + levelMegaBytesMap +
                '}';
    }
}
