package com.neoKV.neoKVServer.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Set;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class MetaConfig {
    @JsonProperty("level")
    @JsonDeserialize(as = TreeMap.class)
    private TreeMap<Integer, Integer> level;

    public Set<Integer> allLevels() {
        return level.keySet();
    }

    public TreeMap<Integer, Integer> getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "MetaConfig{" +
                "level=" + level +
                '}';
    }
}

