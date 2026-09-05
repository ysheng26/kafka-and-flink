package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class AlcoholPriceDeserializer implements KafkaRecordDeserializationSchema<AlcoholPrice> {
    private final ObjectMapper mapper = new ObjectMapper();

    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<AlcoholPrice> out) throws IOException {
        if (record.value() == null) {
            return;
        }
        AlcoholPrice price = mapper.readValue(record.value(), AlcoholPrice.class);

        out.collect(price);
    }

    @Override
    public TypeInformation<AlcoholPrice> getProducedType() {
        return TypeInformation.of(AlcoholPrice.class);
    }
}
