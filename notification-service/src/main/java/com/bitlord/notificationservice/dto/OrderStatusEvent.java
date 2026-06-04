package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class OrderStatusEvent {
    private String eventType;
    private String orderId;
    private String customerEmail;
    private String previousStatus;
    private String newStatus;
    private LocalDateTime timestamp;
}
