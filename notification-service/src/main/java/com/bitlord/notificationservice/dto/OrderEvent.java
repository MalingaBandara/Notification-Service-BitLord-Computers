package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
public class OrderEvent {
    private String eventType;
    private String orderId;
    private String customerId;
    private String customerEmail;
    private List<OrderItemDto> items;
    private BigDecimal totalAmount;
    private LocalDateTime timestamp;

    @Data
    public static class OrderItemDto {
        private String sku;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
    }
}
