// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.factory.producer;

import com.google.common.collect.ImmutableMap;
import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import java.io.Closeable;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.gobblin.configuration.State;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


//TODO: Implement Kafka Rest client instead
public class KafkaEventReporter implements EventReporter<IndexedRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaEventReporter.class);
  private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  private static final String VALUE_SERIALIZER = "value.serializer";
  private static final String KEY_SERIALIZER = "key.serializer";
  private Producer producer;
  private State state;
  private String topicName;
  // Translate essential Kafka producer config from ms parameters. More kafka producer configs can pass based on open source kafka documentation
  ImmutableMap<String, String> kafkaConfigMapping = ImmutableMap.<String, String>builder()
      .put(MSTAGE_KAFKA_BROKERS.toString(), BOOTSTRAP_SERVERS)
      .put(MSTAGE_KAFKA_VALUE_SERIALIZER.toString(), VALUE_SERIALIZER)
      .put(MSTAGE_KAFKA_KEY_SERIALIZER.toString(), KEY_SERIALIZER)
      .build();

  public KafkaEventReporter(State state) {
    this.state = state;
    this.topicName = MSTAGE_KAFKA_EVENT_TOPIC_NAME.get(state);
    fillKafkaProducerConfig(state);
  }

  private void fillKafkaProducerConfig(State state) {
    for (Map.Entry<String, String> entry: kafkaConfigMapping.entrySet()) {
      if (state.contains(entry.getKey())) {
        state.setProp(entry.getValue(), state.getProp(entry.getKey()));
      } else if (entry.getKey().equalsIgnoreCase(MSTAGE_KAFKA_BROKERS.toString())) {
        LOG.error("{} can't be blank when audit/metrics is enabled", entry.getKey());
      }
    }
  }

  private Producer getProducer() {
    if (producer != null) {
      return producer;
    }
    return new LiKafkaProducerImpl<String, IndexedRecord>(this.state.getProperties());
  }

  @Override
  public void send(IndexedRecord event) {
    if (producer == null) {
      producer = getProducer();
      LOG.info("Kafka producer is initialized");
    }
    LOG.info("Sending Event {} to topic {} " , event.get(2), topicName);
    producer.send(new ProducerRecord(topicName, event));
  }

  @Override
  public void close() {
    if (producer != null && producer instanceof Closeable) {
      producer.close();
    }
  }
}
