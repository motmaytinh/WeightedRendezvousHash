package com.google.common.hash;

public class AlwaysOneHashFunction extends AbstractNonStreamingHashFunction {

    public int bits() {
        return 64;
    }

    public HashCode hashBytes(byte[] arg0, int arg1, int arg2) {
        return HashCode.fromLong(1);
    }

}

