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

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Generic consumer factory builder — each topic gets its own typed factory.
     */
    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> targetType, String groupId) {
        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(targetType);
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setUseTypeHeaders(false); // ignore __TypeId__ header

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderEventFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(consumerFactory(OrderEvent.class, "notification-group"));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderStatusEvent> orderStatusEventFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderStatusEvent>();
        factory.setConsumerFactory(consumerFactory(OrderStatusEvent.class, "notification-group"));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResult> inventoryResultFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResult>();
        factory.setConsumerFactory(consumerFactory(InventoryReservationResult.class, "notification-group"));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LowStockAlert> lowStockAlertFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, LowStockAlert>();
        factory.setConsumerFactory(consumerFactory(LowStockAlert.class, "notification-group"));
        return factory;
    }
}