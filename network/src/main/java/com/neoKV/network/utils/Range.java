package com.neoKV.network.utils;

/**
 * @author neo82
 */
public class Range<T> {
    private final T start;
    private final T end;

    public Range(T start, T end) {
        this.start = start;
        this.end = end;
    }

    public static <T> Range<T> of(T start, T end) {
        return new Range<>(start, end);
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "Range{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
