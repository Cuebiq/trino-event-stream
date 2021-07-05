/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.eventstream;

import io.airlift.log.Logger;
import io.trino.spi.eventlistener.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * An EventListener wraps Kafka producer to send query events to Kafka
 */
public class EventStreamEventListener implements EventListener {

    private static final Logger log = Logger.get(EventStreamEventListener.class);
    private static final String TRINO_EVENT_TOPIC_CONF_KEY = "event.topic";
    private final KafkaProducer kafkaProducer;
    private final String trinoEventTopic;

    public EventStreamEventListener(
            KafkaProducer<String, Object> kafkaProducer,
            Map<String, Object> eventStreamConfig
    )
    {
        this.kafkaProducer = kafkaProducer;
        trinoEventTopic = eventStreamConfig.get(TRINO_EVENT_TOPIC_CONF_KEY).toString();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            queryCreatedEvent.toString(),
                            QueryCreatedEventConverter.convert(queryCreatedEvent).toString()
                    )
            );
            log.debug("Sent queryCreated event. Query id: %s", queryCreatedEvent.getMetadata().getQueryId());
        }
        catch (Exception e) {
            log.error("Exception in sending queryCreated event. Query id: %s", queryCreatedEvent.getMetadata().getQueryId());
            log.error(e);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            queryCompletedEvent.toString(),
                            QueryCompletedEventConverter.convert(queryCompletedEvent).toString())
            );
            log.debug("Sent queryCompleted event. Query id: %s", queryCompletedEvent.getMetadata().getQueryId());
        }
        catch (Exception e) {
            log.error("Exception in sending queryCompleted event. Query id: %s", queryCompletedEvent.getMetadata().getQueryId());
            log.error(e);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            splitCompletedEvent.toString(),
                            SplitCompletedEventConverter.convert(splitCompletedEvent).toString())
            );
            log.debug("Sent splitCompleted event. Query id %s", splitCompletedEvent.getQueryId());
        }
        catch (Exception e) {
            log.error("Exception in sending splitCompleted event. Query id: %s", splitCompletedEvent.getQueryId());
            log.error(e);
        }
    }
}
