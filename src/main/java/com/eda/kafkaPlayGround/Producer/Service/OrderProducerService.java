package com.eda.kafkaPlayGround.Producer.Service;


import com.eda.kafkaPlayGround.Producer.Model.Order;
import com.eda.kafkaPlayGround.Producer.Model.Payment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

//    @Autowired
//    private KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateStringValue;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplateWithObject;

//    @Autowired
//    @Qualifier("customSerializer")
//    private KafkaTemplate<String, Order> customSerializerKafkaTemplate;

//    public void sendWithKey(Order order){
//        String key = order.getOrderId();
//
//        CompletableFuture<SendResult<String, Order>> future = kafkaTemplate.send("order-events", key, order);
//
//        future.whenComplete((result, ex) -> {
//           if(ex == null) {
//               System.out.println("Order event created successfully");
//               System.out.println("Partition:: " + result.getRecordMetadata().partition());
//               System.out.println("Offset:: " + result.getRecordMetadata().offset());
//           } else{
//               System.out.println("Failed to create kafka topic :: " + ex.getMessage());
//           }
//        });
//    }

//    public void sendWithKeyForCustomSerializer(Order order){
//        String key = order.getOrderId();
//
//        CompletableFuture<SendResult<String, Order>> future = customSerializerKafkaTemplate.send("order-events", key, order);
//        future.whenComplete((result, ex) ->{
//            if(ex == null) {
//                System.out.println("Order event created successfully");
//                System.out.println("Partition:: " + result.getRecordMetadata().partition());
//                System.out.println("Offset:: " + result.getRecordMetadata().offset());
//            } else{
//                System.out.println("Failed to create kafka topic :: " + ex.getMessage());
//            }
//        });
//    }

    public void sendWithKeyObject(Order order){
        String key = order.getOrderId();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplateWithObject.send("order-events", key, order);

        future.whenComplete((result, ex) -> {
           if(ex == null){
               System.out.println("Order event created successfully");
               System.out.println("Partition:: " + result.getRecordMetadata().partition());
               System.out.println("Offset:: " + result.getRecordMetadata().offset());
           } else{
               System.out.println("Failed to create kafka topic :: " + ex.getMessage());
           }
        });
    }

    public void sendBugeeMessage(Order order){
        String key = order.getOrderId();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplateStringValue.send("order-events", key, "This is string not a JSON");

        future.whenComplete((result, ex) -> {
            if(ex == null) {
                System.out.println("Order event created successfully");
                System.out.println("Partition:: " + result.getRecordMetadata().partition());
                System.out.println("Offset:: " + result.getRecordMetadata().offset());
            } else{
                System.out.println("Failed to create kafka topic :: " + ex.getMessage());
            }
        });
    }

    public String sendTransactionCommit(Order order, Payment payment){
        return kafkaTemplateWithObject.executeInTransaction(ops -> {
            //Send 1 event.
            SendResult<String, Object> orderResult = ops.send("order-events", order.getOrderId(), order).join();
            System.out.println("Order written in partition " +
                    orderResult.getRecordMetadata().partition() +
                    ", offset " + orderResult.getRecordMetadata().offset());


            //Send 2 event.
            SendResult<String, Object> paymentResult = ops.send("payment-events", payment.getPaymentId(), payment).join();
            System.out.println("Payment written in partition " +
                    paymentResult.getRecordMetadata().partition() +
                    ",offset " + paymentResult.getRecordMetadata().offset());


            return "COMMITTED";
        });
    }

    public String sendTransactionAbort(Order order){
        try {
            return kafkaTemplateWithObject.executeInTransaction(ops -> {
                SendResult<String, Object> orderResult = ops.send("order-events", order.getOrderId()).join();
                System.out.println("Order written in partition " +
                        orderResult.getRecordMetadata().partition() +
                        ", offset " + orderResult.getRecordMetadata().offset());
                throw new RuntimeException("Failure, : Transaction was aborted");
            });
        }
       catch(Exception e){
            return "ABORTED" + e.getMessage();
       }
    }

}
