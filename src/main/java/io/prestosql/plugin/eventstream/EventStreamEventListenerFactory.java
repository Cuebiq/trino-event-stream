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
package io.prestosql.plugin.eventstream;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

public class EventStreamEventListenerFactory implements EventListenerFactory {
    private static final Logger log = Logger.get(EventStreamEventListenerFactory.class);

    private static final String KAFKA_PRODUCER_CONFIG_PREXIF = "kafka.producer.";
    private static final String EVENT_LISTENER_CONFIG_PREFIX = "cuebiq.event.listener.";

    @Override
    public String getName()
    {
        return "cuebiq-prestosql-event-stream";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        Map<String, Object> kafkaProducerConf = toSpecificConfig(config, KAFKA_PRODUCER_CONFIG_PREXIF);
        Map<String, Object> eventListenerConf = toSpecificConfig(config, EVENT_LISTENER_CONFIG_PREFIX);

        KafkaProducer<String, Object> kafkaProducer = createKafkaProducer(kafkaProducerConf);

        return new EventStreamEventListener(kafkaProducer, eventListenerConf);
    }

    private static Map<String, Object> toSpecificConfig(
            Map<String, String> config,
            String specificPrefix
    ) {
        ImmutableMap.Builder builder = ImmutableMap.<String, Object>builder();

        for (String key : config.keySet()) {
            if (key.startsWith(specificPrefix)) {
                log.info("Loading config %s", key);
                builder.put(key.substring(specificPrefix.length()), config.get(key));
            }
        }

        return builder.build();
    }

    private KafkaProducer<String, Object> createKafkaProducer(Map<String, Object> properties)
    {
        return new KafkaProducer<>(properties);
    }
}
