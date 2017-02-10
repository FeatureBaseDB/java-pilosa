package com.pilosa.client;

public class CountResult {
    private int key;
    private int count;

    public CountResult(int key, int count) {
        this.key = key;
        this.count = count;
    }

    public int getKey() {
        return this.key;
    }

    public int getCount() {
        return this.count;
    }

    @Override
    public String toString() {
        return String.format("CountResult(key=%d, count=%d)", this.key, this.count);
    }
}
