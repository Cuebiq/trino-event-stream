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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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
        String queryId = queryCreatedEvent.getMetadata().getQueryId();
        if (!queryCreatedEnabled) {
            log.debug("Ignored QUERY_CREATED event. Query id: %s", queryId);
            return;
        }
        QueryCreatedEventV1 queryCreatedEventV1 = QueryCreatedEventConverter.convert(queryCreatedEvent);
        kafkaProducer.send(
                new ProducerRecord<>(
                        trinoEventTopic,
                        queryId,
                        queryCreatedEventV1.toString()
                ),
                sendCallbackFunction(queryCreatedEventV1.getEventName(), queryId)
        );
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        if (!queryCompletedEnabled) {
            log.debug("Ignored QUERY_COMPLETED event. Query id: %s", queryId);
            return;
        }
        QueryCompletedEventV1 queryCompletedEventV1 = QueryCompletedEventConverter.convert(queryCompletedEvent);
        kafkaProducer.send(
                new ProducerRecord<>(
                        trinoEventTopic,
                        queryId,
                        queryCompletedEventV1.toString()
                ),
                sendCallbackFunction(queryCompletedEventV1.getEventName(), queryId)
        );
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        String queryId = splitCompletedEvent.getQueryId();
        if (!splitCompletedEnabled) {
            log.debug("Ignored SPLIT_COMPLETED event. Query id: %s", queryId);
            return;
        }
        SplitCompletedEventV1 splitCompletedEventV1 = SplitCompletedEventConverter.convert(splitCompletedEvent);
        kafkaProducer.send(
                new ProducerRecord<>(
                        trinoEventTopic,
                        queryId,
                        splitCompletedEventV1.toString()
                ),
                sendCallbackFunction(splitCompletedEventV1.getEventName(), queryId)
        );
    }

    private static Callback sendCallbackFunction(String eventName, String queryId) {
        return new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    log.error("Exception in sending %s event. Query id: %s", eventName, queryId);
                    log.error(e);
                }
                else {
                    log.debug(
                            "Sent %s event to topic %s, partition %s, offset %s. Query id: %s",
                            eventName,
                            queryId,
                            metadata.topic(),
                            String.valueOf(metadata.partition()),
                            String.valueOf(metadata.offset())
                    );
                }
            }
        };
    }
}
