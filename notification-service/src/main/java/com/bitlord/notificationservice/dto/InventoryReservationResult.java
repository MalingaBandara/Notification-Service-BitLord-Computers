package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.time.LocalDateTime;


/**
 * DTO representing the result of an inventory reservation event.
 *
 * This object is typically used in event-driven communication
 * between services (e.g., Order Service → Notification Service)
 * to inform whether inventory reservation succeeded or failed.
 */
@Data
public class InventoryReservationResult {

    // Type of the event (e.g., INVENTORY_RESERVED, INVENTORY_FAILED)
    private String eventType;

    // Unique identifier of the order related to this event
    private String orderId;

    // Status of the inventory reservation (e.g., SUCCESS, FAILED)
    private String status;

    // Reason for failure if reservation was not successful
    private String reason;

    // Timestamp when the event was created/published
    private LocalDateTime timestamp;
}