package com.pm.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @PostConstruct
    public void init() {
        log.info("🔥 Kafka consumer bean initialized");
    }

    @KafkaListener(topics = "patient", groupId = "analytics-service")
    public void listen(byte[] message) {
        log.info("🔥 RECEIVED MESSAGE: {}", new String(message));
    }

    @KafkaListener(topics = "patient", groupId = "analytics-service-2")
    public void consumeEvent(ConsumerRecord<String, byte[]> record) {

        log.info("🔥 RECEIVED MESSAGE");

        byte[] event = record.value();

        try {
            log.info("Received raw event of size: {}", event.length);

            PatientEvent patientEvent = PatientEvent.parseFrom(event);
            //Perform analytics business logic here.

            log.info("Received Patient Event: [PatientId={}, PatientName={}, PatientEmail={} ]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail());

        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing event {}", e.getMessage());
        }

    }
}
