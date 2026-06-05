package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.time.LocalDateTime;


/**
 * DTO representing a low stock alert event.
 *
 * This object is used to carry inventory warning data
 * when product stock falls below a defined threshold.
 */
@Data
public class LowStockAlert {

    // Type of event (e.g., LOW_STOCK_ALERT)
    private String eventType;

    // SKU (Stock Keeping Unit) identifier of the product
    private String sku;

    // Human-readable product name
    private String productName;

    // Current available stock quantity
    private Integer currentStock;

    // Minimum threshold that triggers the alert
    private Integer threshold;

    // Timestamp when the alert event was generated
    private LocalDateTime timestamp;
}