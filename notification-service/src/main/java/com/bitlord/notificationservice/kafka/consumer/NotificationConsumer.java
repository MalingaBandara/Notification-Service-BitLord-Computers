package com.bitlord.notificationservice.kafka.consumer;

import com.bitlord.notificationservice.dto.InventoryReservationResult;
import com.bitlord.notificationservice.dto.LowStockAlert;
import com.bitlord.notificationservice.dto.OrderEvent;
import com.bitlord.notificationservice.dto.OrderStatusEvent;
import com.bitlord.notificationservice.email.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Universal consumer that eavesdrops on all major ecosystem events.
 * Maps these events to direct messaging tasks using EmailService.
 * Stateless microservice component that guarantees separation of concerns.
 */
@Service
public class NotificationConsumer {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    private final EmailService emailService;

    public NotificationConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    /**
     * Reacts directly as soon as User commits their purchase intent.
     */
    @KafkaListener(topics = "order-placed", groupId = "notification-group", containerFactory = "orderEventFactory")
    public void consumeOrderPlacedEvent(OrderEvent event) {
        log.info("Notification: Order Placed Event received: {}", event);
        if (event.getCustomerEmail() != null) {
            java.util.Map<String, Object> model = new java.util.HashMap<>();
            model.put("orderId", event.getOrderId());
            model.put("items", event.getItems());
            model.put("totalAmount", event.getTotalAmount());
            emailService.sendHtmlEmail(event.getCustomerEmail(),
                    "Order Placed: " + event.getOrderId(),
                    "order-placed", model);
        }
    }

    /**
     * Intercepts status cascades (e.g. PENDING -> CONFIRMED / FAILED). 
     * Informs end users about their package state dynamically over time.
     */
    @KafkaListener(topics = "order-status-updated", groupId = "notification-group", containerFactory = "orderStatusEventFactory")
    public void consumeOrderStatusUpdatedEvent(OrderStatusEvent event) {
        log.info("Notification: Order Status Updated Event received: {}", event);
        if (event.getCustomerEmail() != null) {
            java.util.Map<String, Object> model = new java.util.HashMap<>();
            model.put("orderId", event.getOrderId());
            model.put("newStatus", event.getNewStatus());
            
            String templateName = "order-updated"; // default fallback
            String subject = "Order Status Update: " + event.getOrderId();
            
            if ("CONFIRMED".equalsIgnoreCase(event.getNewStatus())) {
                templateName = "order-confirmed";
                subject = "✅ Your order has been confirmed — " + event.getOrderId();
            } else if ("FAILED".equalsIgnoreCase(event.getNewStatus())) {
                templateName = "order-rejected";
                subject = "⚠️ We couldn't complete your order — " + event.getOrderId();
            } else if ("SHIPPED".equalsIgnoreCase(event.getNewStatus())) {
                templateName = "order-shipped";
                subject = "🚚 Your order is on its way!";
            } else if ("DELIVERED".equalsIgnoreCase(event.getNewStatus())) {
                templateName = "order-delivered";
                subject = "📦 Order delivered — Thank you!";
            }

            emailService.sendHtmlEmail(event.getCustomerEmail(), subject, templateName, model);
        }
    }

    /**
     * Monitors inventory transaction streams safely.
     */
    @KafkaListener(topics = "inventory-reservation-result", groupId = "notification-group", containerFactory = "inventoryResultFactory")
    public void consumeInventoryReservationResult(InventoryReservationResult event) {
        log.info("Notification: Inventory Reservation Result received: {}", event);
        // This is caught by Order system internally to transition status. 
        // We can safely omit emailing here because 'order-status-updated' picks up the transition.
    }

    /**
     * Informs internal enterprise warehouse operators of system deficits.
     */
    @KafkaListener(topics = "low-stock-alert", groupId = "notification-group", containerFactory = "lowStockAlertFactory")
    public void consumeLowStockAlert(LowStockAlert event) {
        log.info("Notification: Low Stock Alert received: {}", event);
        java.util.Map<String, Object> model = new java.util.HashMap<>();
        model.put("productName", event.getProductName());
        model.put("sku", event.getSku());
        model.put("currentStock", event.getCurrentStock());
        model.put("threshold", event.getThreshold());
        
        emailService.sendHtmlEmail("admin@bitlord-computer-parts.com",
                "🔴 Low Stock Alert — " + event.getSku(),
                "low-stock-alert", model);
    }
}
