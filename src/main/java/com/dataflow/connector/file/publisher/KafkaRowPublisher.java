package com.dataflow.connector.file.publisher;

import com.dataflow.connector.file.model.RowMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaRowPublisher {

    // FIX: Replaced System.out/err with SLF4J Logger
    private static final Logger logger = LoggerFactory.getLogger(KafkaRowPublisher.class);

    @Value("${file-connector.topic:excel-file-updates}")
    private String topic;

    private final KafkaTemplate<String, RowMessage> kafkaTemplate;

    public KafkaRowPublisher(KafkaTemplate<String, RowMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(RowMessage rowMessage) {
        // FIX: Construct message key as filename:rowIndex for partition ordering
        String messageKey = rowMessage.getFileName() + ":" + rowMessage.getRowIndex();

        kafkaTemplate.send(topic, messageKey, rowMessage).whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Published row [{}] key [{}] to topic [{}]", 
                        rowMessage.getRowIndex(), messageKey, topic);
            } else {
                logger.error("Failed to publish row [{}] key [{}] due to: {}", 
                        rowMessage.getRowIndex(), messageKey, ex.getMessage(), ex);
            }
        });
    }
}