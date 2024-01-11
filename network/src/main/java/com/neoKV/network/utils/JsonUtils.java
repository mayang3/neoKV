package com.neoKV.network.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * @author neo82
 */
public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void writeValue(Writer w, Object o) throws IOException {
        objectMapper.writeValue(w, o);
    }

    public static <T> T readValue(File file, Class<T> valueType) throws IOException {
        return objectMapper.readValue(file, valueType);
    }
}
