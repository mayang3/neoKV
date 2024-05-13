package com.neoKV.network.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author neo82
 */
public class RangeIntegerDeserializer extends JsonDeserializer<List<Range<Integer>>> {

    @Override
    public List<Range<Integer>> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ArrayNode arrayNode = p.getCodec().readTree(p);
        List<Range<Integer>> ranges = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            String startStr = node.get("start").asText().substring(2);
            String endStr = node.get("end").asText().substring(2);
            Integer start = Integer.parseInt(startStr, 16);
            Integer end = Integer.parseInt(endStr, 16);
            ranges.add(new Range<>(start, end));
        }
        return ranges;
    }
}
