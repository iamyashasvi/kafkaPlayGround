package com.eda.kafkaPlayGround.Producer.serializer;

import com.eda.kafkaPlayGround.Producer.Model.Order;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.json.JsonParseException;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

//First option we have to define in application-properties
//spring.kafka.producer.value-serializer=com.eda.kafkaPlayGround.producer.serializer.OrderSummarySerializer

//Seconds option we have to define using bean. --> This is the best i guess.
public class OrderSummarySerializer implements Serializer<Order> {

    private final ObjectMapper objectMapper;

    public OrderSummarySerializer(){
        this(new ObjectMapper());
    }

    public OrderSummarySerializer(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }


    @Override
    public byte[] serialize(String topic, Order order) {
        if(order == null){
            return null;
        }
        try{
            Map<String, Object> kafkaToConvert = new LinkedHashMap<>();
            kafkaToConvert.put("orderId", order.getOrderId());
            return objectMapper.writeValueAsBytes(kafkaToConvert);
        }catch (JsonParseException jPE){
            System.out.println("Something went wrong " + jPE.getMessage());
            throw new RuntimeException("Failed to execute...", jPE);
        }
    }
}
