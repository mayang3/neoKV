package com.neoKV.neoKVServer.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.neoKV.network.utils.Range;
import com.neoKV.network.utils.RangeIntegerDeserializer;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class MetaConfig {
    @JsonProperty("level")
    @JsonDeserialize(as = TreeMap.class)
    private TreeMap<Integer, Integer> level;

    @JsonProperty("port")
    private int port;

    @JsonProperty("table-range")
    @JsonDeserialize(using = RangeIntegerDeserializer.class)
    private List<Range<Integer>> tableRangeList;

    public Set<Integer> allLevels() {
        return level.keySet();
    }

    public int getMinLevel() {
        return level.firstKey();
    }

    public int getMaxLevel() {
        return level.lastKey();
    }

    public TreeMap<Integer, Integer> getLevel() {
        return level;
    }

    public int getPort() {
        return port;
    }

    public List<Range<Integer>> getTableRangeList() {
        return tableRangeList;
    }

    @Override
    public String toString() {
        return "MetaConfig{" +
                "level=" + level +
                ", port=" + port +
                ", tableRangeList=" + tableRangeList +
                '}';
    }
}

