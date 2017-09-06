package com.ryft.elasticsearch.plugin;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentType;

public class ObjectMapperFactory {

    private final Map<XContentType, ObjectMapper> mappers = new HashMap<>();

    public ObjectMapperFactory() {
        JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
        ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
        objectMapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
        mappers.put(XContentType.JSON, objectMapper);

        jsonFactory = new CBORFactory();
        jsonFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
        objectMapper = new ObjectMapper(jsonFactory);
        objectMapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
        mappers.put(XContentType.CBOR, objectMapper);

        objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
        mappers.put(XContentType.YAML, objectMapper);

        jsonFactory = new SmileFactory();
        ((SmileFactory) jsonFactory).configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
        jsonFactory.configure(SmileFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
        objectMapper = new ObjectMapper(jsonFactory);
        mappers.put(XContentType.SMILE, objectMapper);
    }

    public ObjectMapper get(XContentType xContentType) {
        return mappers.get(xContentType);
    }

    public ObjectMapper get() {
        return mappers.get(XContentType.JSON);
    }

}
