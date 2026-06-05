package com.bitlord.notificationservice.kafka.config;

import com.bitlord.notificationservice.dto.InventoryReservationResult;
import com.bitlord.notificationservice.dto.LowStockAlert;
import com.bitlord.notificationservice.dto.OrderEvent;
import com.bitlord.notificationservice.dto.OrderStatusEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;


// Kafka consumer configuration class — defines consumer factories for each event type
@Configuration
public class KafkaConsumerConfig {

    // Kafka broker address injected from application.properties/yml
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Generic consumer factory builder — each topic gets its own typed factory.
     *
     * @param targetType - the DTO class the JSON message will be deserialized into
     * @param groupId    - the Kafka consumer group this factory belongs to
     */
    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> targetType, String groupId) {

        // Create a JSON deserializer for the given target DTO type
        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(targetType);

        // Trust all packages during deserialization — avoids package mismatch errors between producer and consumer
        jsonDeserializer.addTrustedPackages("*");

        // Ignore the __TypeId__ header sent by the producer — use targetType directly instead
        jsonDeserializer.setUseTypeHeaders(false);

        // Build the consumer configuration properties map
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);  // Kafka broker address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);                     // Consumer group ID
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");         // Start reading from the earliest offset if no committed offset exists

        // Wrap both deserializers in ErrorHandlingDeserializer to prevent the consumer from crashing on bad messages
        return new DefaultKafkaConsumerFactory<>(
                props,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),   // Key deserializer — message key is a plain String
                new ErrorHandlingDeserializer<>(jsonDeserializer)            // Value deserializer — message value is deserialized as the target DTO
        );
    }

    // Listener container factory for consuming OrderEvent messages from the "order-placed" topic
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderEventFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(consumerFactory(OrderEvent.class, "notification-group"));
        return factory;
    }

    // Listener container factory for consuming OrderStatusEvent messages from the "order-status-updated" topic
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderStatusEvent> orderStatusEventFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderStatusEvent>();
        factory.setConsumerFactory(consumerFactory(OrderStatusEvent.class, "notification-group"));
        return factory;
    }

    // Listener container factory for consuming InventoryReservationResult messages from the inventory reservation topic
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResult> inventoryResultFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResult>();
        factory.setConsumerFactory(consumerFactory(InventoryReservationResult.class, "notification-group"));
        return factory;
    }

    // Listener container factory for consuming LowStockAlert messages from the low stock alert topic
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LowStockAlert> lowStockAlertFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, LowStockAlert>();
        factory.setConsumerFactory(consumerFactory(LowStockAlert.class, "notification-group"));
        return factory;
    }
}