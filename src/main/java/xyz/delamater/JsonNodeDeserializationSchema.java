package xyz.delamater;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class JsonNodeDeserializationSchema implements DeserializationSchema<EditEvent>, ResultTypeQueryable<EditEvent> {

    private static final long serialVersionUID = -3859751518685780456L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public EditEvent deserialize(byte[] message) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(message);
        EditEvent editEvent = new EditEvent();
        editEvent.setId(jsonNode.get("id").asText());
        editEvent.setDomain(jsonNode.get("domain").asText());
        editEvent.setNamespace(jsonNode.get("namespace").asText());
        editEvent.setTitle(jsonNode.get("title").asText());
        editEvent.setTimestamp(jsonNode.get("timestamp").asText());
        editEvent.setUser_name(jsonNode.get("user_name").asText());
        editEvent.setUser_type(jsonNode.get("user_type").asText());
        editEvent.setOld_length(Long.parseLong(jsonNode.get("old_length").asText()));
        editEvent.setNew_length(Long.parseLong(jsonNode.get("new_length").asText()));
        return editEvent;
    }

    @Override
    public void deserialize(byte[] message, Collector<EditEvent> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(EditEvent editEvent) {
        return false;
    }

    @Override
    public TypeInformation<EditEvent> getProducedType() {
        return TypeInformation.of(EditEvent.class);
    }
}
