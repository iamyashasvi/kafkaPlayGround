package com.eda.kafkaPlayGround.Producer.Controller;
import com.eda.kafkaPlayGround.Producer.Model.Order;
import com.eda.kafkaPlayGround.Producer.Service.OrderProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/api/orders")
public class OrderController {


    @Autowired
    OrderProducerService orderProducerService;

    /*
    curl --location 'http://localhost:8081/api/orders/with-key' \
        --header 'Content-Type: application/json' \
        --data '{
            "orderId": "1"
        }'
     */
    @PostMapping("/with-key")
    public ResponseEntity<String> sendWithKey(@RequestBody Order order){
        orderProducerService.sendWithKey(order);
        return ResponseEntity.accepted().body("Order created and published successfully!!");
    }

    /*
    curl --location 'http://localhost:8081/api/orders/for-custom-serializer' \
        --header 'Content-Type: application/json' \
        --data '{
            "orderId": "1"
        }'
     */
//    @PostMapping("/for-custom-serializer")
//    public ResponseEntity<String> postMethodName(@RequestBody Order order) {
//        orderProducerService.sendWithKeyForCustomSerializer(order);
//        return ResponseEntity.accepted().body("order created and published through custom serializer");
//    }

    /*
    curl --location 'http://localhost:8081/api/orders/with-key-ObjectType' \
         --header 'Content-Type: application/json' \
        --data '{
            "orderId": "1"
        }'
     */
    @PostMapping("/with-key-ObjectType")
    public ResponseEntity<String> sendWithKeyObject(@RequestBody Order order){
        orderProducerService.sendWithKeyObject(order);
        return ResponseEntity.accepted().body("Order created and published with Object type in header");
    }
    
}
