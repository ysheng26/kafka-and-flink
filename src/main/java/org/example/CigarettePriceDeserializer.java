package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class CigarettePriceDeserializer implements KafkaRecordDeserializationSchema<CigarettePrice> {
    private final ObjectMapper mapper = new ObjectMapper();

    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CigarettePrice> out) throws IOException {
        if (record.value() == null) {
            return;
        }
        CigarettePrice price = mapper.readValue(record.value(), CigarettePrice.class);
        price.setTopic(record.topic());
        price.setOffset(record.offset());
        price.setPartition(record.partition());
        out.collect(price);
    }

    @Override
    public TypeInformation<CigarettePrice> getProducedType() {
        return TypeInformation.of(CigarettePrice.class);
    }
}
