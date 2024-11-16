package deserializer;

import DTO.Appointment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<Appointment> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Appointment deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Appointment.class);
    }

    @Override
    public boolean isEndOfStream(Appointment appointment) {
        return false;
    }

    @Override
    public TypeInformation<Appointment> getProducedType() {
        return TypeInformation.of(Appointment.class);
    }
}