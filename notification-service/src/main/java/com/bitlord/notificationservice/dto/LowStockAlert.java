package com.bitlord.notificationservice.dto;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class LowStockAlert {
    private String eventType;
    private String sku;
    private String productName;
    private Integer currentStock;
    private Integer threshold;
    private LocalDateTime timestamp;
}
