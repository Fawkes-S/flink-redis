package com.demo.flink.model;


public class Order {
    private Long orderId;
    private Long userId;
    private String orderStatus;
    private String destLng;
    private String destLat;
    private String srcLng;
    private String srcLat;
    private String carType;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    private Long timestamp;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getDestLng() {
        return destLng;
    }

    public void setDestLng(String destLng) {
        this.destLng = destLng;
    }

    public String getDestLat() {
        return destLat;
    }

    public void setDestLat(String destLat) {
        this.destLat = destLat;
    }

    public String getSrcLng() {
        return srcLng;
    }

    public void setSrcLng(String srcLng) {
        this.srcLng = srcLng;
    }

    public String getSrcLat() {
        return srcLat;
    }

    public void setSrcLat(String srcLat) {
        this.srcLat = srcLat;
    }

    public String getCarType() {
        return carType;
    }

    public void setCarType(String carType) {
        this.carType = carType;
    }
}
