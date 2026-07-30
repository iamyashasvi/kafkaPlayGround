package com.eda.kafkaPlayGround.Producer.Config;

import com.eda.kafkaPlayGround.Producer.Model.Order;
import com.eda.kafkaPlayGround.Producer.serializer.OrderSummarySerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic createOrderEvents(){
        return TopicBuilder.name("order-events")
                .partitions(4)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824")
                .config(TopicConfig.COMPRESSION_TYPE_DOC, "snappy")
                .build();
    }

    @Bean
    public KafkaTemplate<String, Order> customSerializer(KafkaProperties kafkaProperties){

        //First load application.properties then load this bean.
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());

        // Override only what we have to change.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSummarySerializer.class);

        DefaultKafkaProducerFactory<String, Order> factory = new DefaultKafkaProducerFactory<>(props);

        return new KafkaTemplate<>(factory);
    }
}
