package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;


/**
 * DTO representing an Order Event sent through the system
 * (typically via messaging like Kafka/RabbitMQ).
 *
 * This is used by the notification service to process and
 * send order-related notifications to users/admins.
 */
@Data
public class OrderEvent {

    // Type of event (e.g., ORDER_CREATED, ORDER_CANCELLED, ORDER_CONFIRMED)
    private String eventType;

    // Unique identifier of the order
    private String orderId;

    // Unique identifier of the customer who placed the order
    private String customerId;

    // Email of the customer for sending notifications
    private String customerEmail;

    // List of items included in the order
    private List<OrderItemDto> items;

    // Total amount of the order
    private BigDecimal totalAmount;

    // Timestamp when the event was created
    private LocalDateTime timestamp;


    /**
     * Inner DTO representing individual items inside an order
     */
    @Data
    public static class OrderItemDto {

        // Stock Keeping Unit (unique product identifier)
        private String sku;

        // Name of the product
        private String productName;

        // Quantity of the product ordered
        private Integer quantity;

        // Price per single unit of the product
        private BigDecimal unitPrice;
    }
}