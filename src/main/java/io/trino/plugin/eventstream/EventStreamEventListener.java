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
    private static final String QUERY_CREATED_ENABLED_CONF_KEY = "query.created.enabled";
    private static final String QUERY_COMPLETED_ENABLED_CONF_KEY = "query.completed.enabled";
    private static final String SPLIT_COMPLETED_ENABLED_CONF_KEY = "split.completed.enabled";

    private static final String queryCreatedEnabledDefault = "true";
    private static final String queryCompletedEnabledDefault = "true";
    private static final String splitCompletedEnabledDefault = "false";

    private static final String trinoEventTopicDefault = "trino.event";

    private final KafkaProducer kafkaProducer;
    private final String trinoEventTopic;
    private final boolean queryCreatedEnabled;
    private final boolean queryCompletedEnabled;
    private final boolean splitCompletedEnabled;

    public EventStreamEventListener(
            KafkaProducer<String, Object> kafkaProducer,
            Map<String, Object> eventStreamConfig
    )
    {
        this.kafkaProducer = kafkaProducer;
        trinoEventTopic = eventStreamConfig.getOrDefault(TRINO_EVENT_TOPIC_CONF_KEY, trinoEventTopicDefault).toString();
        queryCreatedEnabled = Boolean.parseBoolean(
                eventStreamConfig
                        .getOrDefault(QUERY_CREATED_ENABLED_CONF_KEY, queryCreatedEnabledDefault)
                        .toString()
        );
        queryCompletedEnabled = Boolean.parseBoolean(
                eventStreamConfig
                        .getOrDefault(QUERY_COMPLETED_ENABLED_CONF_KEY, queryCompletedEnabledDefault)
                        .toString()
        );
        splitCompletedEnabled = Boolean.parseBoolean(
                eventStreamConfig
                        .getOrDefault(SPLIT_COMPLETED_ENABLED_CONF_KEY, splitCompletedEnabledDefault)
                        .toString()
        );
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        if (!queryCreatedEnabled) {
            log.debug("Ignored queryCreated event. Query id: %s", queryCreatedEvent.getMetadata().getQueryId());
            return;
        }
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            queryCreatedEvent.getMetadata().getQueryId(),
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
        if (!queryCompletedEnabled) {
            log.debug("Ignored queryCompleted event. Query id: %s", queryCompletedEvent.getMetadata().getQueryId());
            return;
        }
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            queryCompletedEvent.getMetadata().getQueryId(),
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
        if (!splitCompletedEnabled) {
            log.debug("Ignored splitCompleted event. Query id %s", splitCompletedEvent.getQueryId());
            return;
        }
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            trinoEventTopic,
                            splitCompletedEvent.getQueryId(),
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
