package com.neoKV.network.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author neo82
 */
public class YamlUtils {
    private static final Logger log = LoggerFactory.getLogger(YamlUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    static {
        mapper.findAndRegisterModules();
    }


    public static <T> T readValue(String fileName, Class<T> clazz) {
        try {
            return mapper.readValue(YamlUtils.class.getClassLoader().getResourceAsStream(fileName), clazz);
        } catch (IOException e) {
            log.error("[YamlUtils] readValue error! path:{}, clazz:{}", fileName, clazz, e);
        }

        return null;
    }
}
