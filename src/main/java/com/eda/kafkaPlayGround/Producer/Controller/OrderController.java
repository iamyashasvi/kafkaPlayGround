package com.eda.kafkaPlayGround.Producer.Controller;
import com.eda.kafkaPlayGround.Producer.Model.Order;
import com.eda.kafkaPlayGround.Producer.Model.Payment;
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
//    @PostMapping("/with-key")
//    public ResponseEntity<String> sendWithKey(@RequestBody Order order){
//        orderProducerService.sendWithKey(order);
//        return ResponseEntity.accepted().body("Order created and published successfully!!");
//    }

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
    /*
    curl --location 'http://localhost:8081/api/orders/send-bugee-message' \
        --header 'Content-Type: application/json' \
        --data '{
            "orderId": "1"
        }'
     */
    @PostMapping("/send-bugee-message")
    public ResponseEntity<String> sendBugeeMessage(@RequestBody Order order){
        orderProducerService.sendBugeeMessage(order);
        return ResponseEntity.accepted().body("Order created and published successfully!!");
    }


    @PostMapping("/txn-commit")
    public ResponseEntity<String> txnCommit(@RequestBody Order order){
        /*
        curl --location 'http://localhost:8081/api/orders/txn-commit' \
            --header 'Content-Type: application/json' \
                --data '{
                    "orderId": "0-34",
                    "customerId": "CUST-2",
                    "productId": "PROD-3",
                    "quantity": 1,
                    "totalAmount": 99.99,
                    "status": "CREATED"
                }'
         */
        Payment payment = new Payment(
                "PAY " + order.getOrderId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                " Paid"
        );
        String result = orderProducerService.sendTransactionCommit(order, payment);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/txn-abort")
    /*
            curl --location 'http://localhost:8081/api/orders/txn-abort' \
            --header 'Content-Type: application/json' \
                --data '{
                    "orderId": "0-36",
                    "customerId": "CUST-2",
                    "productId": "PROD-3",
                    "quantity": 1,
                    "totalAmount": 99.99,
                    "status": "CREATED"
                }'
➜  kafka_2.13-4.3.1 bin/kafka-dump-log.sh --deep-iteration --print-data-log --files /tmp/broker1.log/order-events-3/*.log
Dumping /tmp/broker1.log/order-events-3/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: 0 lastSequence: 0 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1791422199843 size: 124 magic: 2 compresscodec: snappy crc: 1059131749 isvalid: true
| offset: 0 CreateTime: 1791422199843 keySize: -1 valueSize: 6 sequence: 0 headerKeys: [__TypeId__] payload: "0-36"
baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: 0 producerEpoch: 1 partitionLeaderEpoch: 0 isTransactional: true isControl: true deleteHorizonMs: OptionalLong.empty position: 124 CreateTime: 1791422200501 size: 78 magic: 2 compresscodec: none crc: 3253606214 isvalid: true
| offset: 1 CreateTime: 1791422200501 keySize: 4 valueSize: 6 sequence: -1 headerKeys: [] endTxnMarker: ABORT coordinatorEpoch: 0
➜  kafka_2.13-4.3.1
     */
    public ResponseEntity<String> txnAbort(@RequestBody Order order){
        String result = orderProducerService.sendTransactionAbort(order);
        return ResponseEntity.ok(result);
    }
}
