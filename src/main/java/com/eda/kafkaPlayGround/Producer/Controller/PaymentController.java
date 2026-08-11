package com.eda.kafkaPlayGround.Producer.Controller;

import com.eda.kafkaPlayGround.Producer.Model.Payment;
import com.eda.kafkaPlayGround.Producer.Service.PaymentProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/payments")
public class PaymentController {


    @Autowired
    PaymentProducerService paymentProducerService;


    /*
    curl --location 'http://localhost:8081/api/payments/sendPaymentEvents' \
        --header 'Content-Type: application/json' \
        --data '{
            "paymentId": 1
           }'
     */
    @PostMapping("/sendPaymentEvents")
    public ResponseEntity<String> sendPaymentEvent(@RequestBody Payment payment){
        paymentProducerService.sendWithKeyPayment(payment);
        return ResponseEntity.accepted().body("Payment Created and published successfully");
    }
}
