package org.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class KafkaRecordToStringDeserializer implements KafkaRecordDeserializationSchema<String> {
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
        String value = new String(record.value());
        StringBuilder sb = new StringBuilder();
        sb.append("topic: ").append(record.topic())
                .append(", partition: ").append(record.partition())
                .append(", offset: ").append(record.offset())
                .append(", value: ").append(value);
        out.collect(sb.toString());
    }

}
