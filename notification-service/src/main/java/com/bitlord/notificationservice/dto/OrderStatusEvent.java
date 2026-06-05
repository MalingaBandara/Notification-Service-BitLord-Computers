package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.time.LocalDateTime;


/**
 * DTO representing an order status change event.
 *
 * This object is typically used in event-driven communication
 * (e.g., Kafka/RabbitMQ) to notify other services when an order
 * status is updated.
 */
@Data
public class OrderStatusEvent {

    // Type of the event (e.g., ORDER_CREATED, ORDER_UPDATED, ORDER_DELIVERED)
    private String eventType;

    // Unique identifier of the order related to this event
    private String orderId;

    // Email of the customer who placed the order
    private String customerEmail;

    // Previous status of the order before the update
    private String previousStatus;

    // New status of the order after the update
    private String newStatus;

    // Timestamp when the event was created/published
    private LocalDateTime timestamp;
}