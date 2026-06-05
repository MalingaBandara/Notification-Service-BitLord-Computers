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

    // Logger for tracking incoming Kafka events and debugging notification flows
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    // Service responsible for sending HTML emails via Thymeleaf templates
    private final EmailService emailService;

    // Constructor injection — wires in the EmailService dependency
    public NotificationConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    /**
     * Reacts directly as soon as User commits their purchase intent.
     */
    // Listens to the "order-placed" Kafka topic and sends an order confirmation email to the customer
    @KafkaListener(topics = "order-placed", groupId = "notification-group", containerFactory = "orderEventFactory")
    public void consumeOrderPlacedEvent(OrderEvent event) {
        log.info("Notification: Order Placed Event received: {}", event);

        // Only send email if a customer email address is available in the event
        if (event.getCustomerEmail() != null) {

            // Build the data model to be injected into the Thymeleaf email template
            java.util.Map<String, Object> model = new java.util.HashMap<>();
            model.put("orderId", event.getOrderId());       // Order reference number
            model.put("items", event.getItems());            // List of ordered items
            model.put("totalAmount", event.getTotalAmount()); // Total order value

            // Send the "order-placed" HTML email using the matching Thymeleaf template
            emailService.sendHtmlEmail(event.getCustomerEmail(),
                    "Order Placed: " + event.getOrderId(),
                    "order-placed", model);
        }
    }

    /**
     * Intercepts status cascades (e.g. PENDING -> CONFIRMED / FAILED).
     * Informs end users about their package state dynamically over time.
     */
    // Listens to the "order-status-updated" Kafka topic and sends a status-specific email to the customer
    @KafkaListener(topics = "order-status-updated", groupId = "notification-group", containerFactory = "orderStatusEventFactory")
    public void consumeOrderStatusUpdatedEvent(OrderStatusEvent event) {
        log.info("Notification: Order Status Updated Event received: {}", event);

        // Only send email if a customer email address is available in the event
        if (event.getCustomerEmail() != null) {

            // Build the data model to be injected into the Thymeleaf email template
            java.util.Map<String, Object> model = new java.util.HashMap<>();
            model.put("orderId", event.getOrderId());       // Order reference number
            model.put("newStatus", event.getNewStatus());   // The updated order status

            String templateName = "order-updated"; // default fallback
            String subject = "Order Status Update: " + event.getOrderId();

            // Select the appropriate email template and subject based on the new order status
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

            // Send the status-specific HTML email using the resolved template and subject
            emailService.sendHtmlEmail(event.getCustomerEmail(), subject, templateName, model);
        }
    }

    /**
     * Monitors inventory transaction streams safely.
     */
    // Listens to the "inventory-reservation-result" Kafka topic
    @KafkaListener(topics = "inventory-reservation-result", groupId = "notification-group", containerFactory = "inventoryResultFactory")
    public void consumeInventoryReservationResult(InventoryReservationResult event) {
        log.info("Notification: Inventory Reservation Result received: {}", event);
        // This is caught by Order system internally to transition status.
        // We can safely omit emailing here because 'order-status-updated' picks up the transition.
    }

    /**
     * Informs internal enterprise warehouse operators of system deficits.
     */
    // Listens to the "low-stock-alert" Kafka topic and sends an alert email to the admin
    @KafkaListener(topics = "low-stock-alert", groupId = "notification-group", containerFactory = "lowStockAlertFactory")
    public void consumeLowStockAlert(LowStockAlert event) {
        log.info("Notification: Low Stock Alert received: {}", event);

        // Build the data model to be injected into the low stock alert email template
        java.util.Map<String, Object> model = new java.util.HashMap<>();
        model.put("productName", event.getProductName()); // Name of the low-stock product
        model.put("sku", event.getSku());                 // SKU identifier of the product
        model.put("currentStock", event.getCurrentStock()); // Current available stock level
        model.put("threshold", event.getThreshold());     // Minimum stock threshold that triggered the alert

        // Send the low stock alert email to the admin inbox
        emailService.sendHtmlEmail("admin@bitlord-computer-parts.com",
                "🔴 Low Stock Alert — " + event.getSku(),
                "low-stock-alert", model);
    }
}