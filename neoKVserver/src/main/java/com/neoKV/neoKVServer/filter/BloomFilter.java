package com.neoKV.neoKVServer.filter;

import com.google.common.hash.Hashing;

import java.util.BitSet;

/**
 * @author neo82
 */
public class BloomFilter {
    private static final int numHashFunctions = 5;
    private final BitSet bitSet;

    public BloomFilter(int size) {
        this.bitSet = new BitSet(size);
    }

    public void put(String key) {
        long hash = Hashing.murmur3_128().newHasher().putString(key).hash().asLong();

        int hash1 = (int)hash;
        int hash2 = (int)(hash >>> 32);

        for(int i = 1; i <= numHashFunctions; ++i) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }

            bitSet.set(nextHash); // TODO size percent
        }
    }

    public boolean mightContains(String key) {
        long hash = Hashing.murmur3_128().newHasher().putString(key).hash().asLong();

        int hash1 = (int)hash;
        int hash2 = (int)(hash >>> 32);

        for(int i = 1; i <= numHashFunctions; ++i) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }

            if (!bitSet.get(nextHash)) {
                return false;
            }
        }

        return true;
    }
}
