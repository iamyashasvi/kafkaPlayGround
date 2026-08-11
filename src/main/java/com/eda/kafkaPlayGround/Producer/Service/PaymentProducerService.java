package com.eda.kafkaPlayGround.Producer.Service;

import com.eda.kafkaPlayGround.Producer.Model.Payment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class PaymentProducerService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendWithKeyPayment(Payment payment){
        String key = payment.getPaymentId();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("payment-events", key, payment);

        future.whenComplete((result, ex) -> {
                if(ex == null){
                    System.out.println("Payment event sent successfully");
                    System.out.println("Partition:: " + result.getRecordMetadata().partition());
                    System.out.println("Offset:: " + result.getRecordMetadata().offset());
                } else {
                    System.out.println("Something went wrong while sending payment event");
                }
        });
    }

}
