package xyz.delamater;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonNodeDeserializationSchema implements DeserializationSchema<JsonNode>, ResultTypeQueryable<JsonNode> {

    private static final long serialVersionUID = -3859751518685780456L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}
