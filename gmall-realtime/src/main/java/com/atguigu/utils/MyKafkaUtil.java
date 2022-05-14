package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {

//        KafkaProducer kafkaProducer = new KafkaProducer<>();
//        kafkaProducer.send(ProducerRecord producerRecord)

        return new FlinkKafkaProducer<T>(
                DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE);
    }

    public static FlinkKafkaConsumer<String> getKakfaSource(String topic, String groupId) {

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return ddl;
    }

}

















