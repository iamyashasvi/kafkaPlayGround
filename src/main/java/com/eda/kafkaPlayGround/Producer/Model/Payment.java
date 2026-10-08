package com.eda.kafkaPlayGround.Producer.Model;

public class Payment {

    public Payment(String paymentId, String customerId, String productId, Integer quantity, Double totalAmount, String status, String countryCode) {
        this.paymentId = paymentId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.status = status;
        this.countryCode = countryCode;
    }

    public Payment(String paymentId, String customerId, Double totalAmount, String status){
        this.paymentId = paymentId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.status = status;
    }

    private String paymentId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private Double totalAmount;
    private String status;
    private String countryCode;

    public Payment(){}


    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }



    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }


}

