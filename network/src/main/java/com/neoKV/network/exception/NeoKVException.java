package com.neoKV.network.exception;

/**
 * @author neo82
 */
public class NeoKVException extends RuntimeException {

    public NeoKVException() {
    }

    public NeoKVException(String message) {
        super(message);
    }

    public NeoKVException(String message, Throwable cause) {
        super(message, cause);
    }

    public NeoKVException(Throwable cause) {
        super(cause);
    }

    public NeoKVException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
