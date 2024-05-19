package com.neoKV.network.utils;

/**
 * @author neo82
 */
public record Range<T>(T start, T end) {

    public static <T> Range<T> of(T start, T end) {
        return new Range<>(start, end);
    }

    @Override
    public String toString() {
        return "Range{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
