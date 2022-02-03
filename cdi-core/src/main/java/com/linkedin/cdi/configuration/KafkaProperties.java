// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Kafka Parameters with SSL parameters
 */
public class KafkaProperties extends JsonObjectProperties {

  private static final String BOOTSTRAP_SERVERS = "bootstrapServers";
  private static final String VALUE_SERIALIZER = "valueSerializer";
  private static final String KEY_SERIALIZER = "keySerializer";
  private static final String CLIENT_ID = "clientId";
  private static final String TOPIC_NAME = "topicName";
  private static final String SCHEMA_REGISTRY_URL = "schemaRegistryUrl";
  private static final String SCHEMA_REGISTRY_CLASS = "schemaRegistryClass";
  private static final String DEFAULT_CLIENT_ID = "cdiKafka";
  private static final String SECURITY_PROTOCOL = "securityProtocol";
  private static final String KEY_STORE_TYPE = "keyStoreType";
  private static final String KEY_STORE_PATH = "keyStorePath";
  private static final String KEY_STORE_PASSWORD = "keyStorePassword";
  private static final String KEY_PASSWORD = "keyPassword";
  private static final String TRUST_STORE_PATH = "trustStorePath";
  private static final String TRUST_STORE_PASSWORD = "trustStorePassword";
  private static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
  private static final String KAFKA_VALUE_SERIALIZER = "value.serializer";
  private static final String KAFKA_KEY_SERIALIZER = "key.serializer";
  private static final String KAFKA_KEY_STORE_TYPE = "ssl.keystore.type";
  private static final String KAFKA_KEY_STORE_PATH = "ssl.keystore.location";
  private static final String KAFKA_KEY_STORE_PASSWORD = "ssl.keystore.password";
  private static final String KAFKA_KEY_PASSWORD = "ssl.key.password";
  private static final String KAFKA_TRUST_STORE_PATH = "ssl.truststore.location";
  private static final String KAFKA_TRUST_STORE_PASSWORD = "ssl.truststore.password";
  private static final String KAFKA_SCHEMA_REGISTRY_CLASS = "kafka.schemaRegistry.class";
  private static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schemaRegistry.url";
  private static final String KAFKA_SECURITY_PROTOCOL = "security.protocol";

  final private static List<String> essentialAttributes =
      Lists.newArrayList(BOOTSTRAP_SERVERS, VALUE_SERIALIZER, KEY_SERIALIZER, TOPIC_NAME, SECURITY_PROTOCOL);

  // Translate essential Kafka producer config from ms parameters. More kafka producer configs can pass based on open source kafka documentation separately
  public static final ImmutableMap<String, String> kafkaConfigMapping =
      ImmutableMap.<String, String>builder().put(BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
          .put(VALUE_SERIALIZER, KAFKA_VALUE_SERIALIZER)
          .put(KEY_SERIALIZER, KAFKA_KEY_SERIALIZER)
          .put(KEY_STORE_TYPE, KAFKA_KEY_STORE_TYPE)
          .put(KEY_STORE_PATH, KAFKA_KEY_STORE_PATH)
          .put(KEY_STORE_PASSWORD, KAFKA_KEY_STORE_PASSWORD)
          .put(KEY_PASSWORD, KAFKA_KEY_PASSWORD)
          .put(TRUST_STORE_PATH, KAFKA_TRUST_STORE_PATH)
          .put(TRUST_STORE_PASSWORD, KAFKA_TRUST_STORE_PASSWORD)
          .put(SCHEMA_REGISTRY_CLASS, KAFKA_SCHEMA_REGISTRY_CLASS)
          .put(SCHEMA_REGISTRY_URL, KAFKA_SCHEMA_REGISTRY_URL)
          .put(SECURITY_PROTOCOL, KAFKA_SECURITY_PROTOCOL)
          .build();

  @Override
  public boolean isValid(State state) {
    if (super.isValid(state) && !super.isBlank(state)) {
      JsonObject value = GSON.fromJson(state.getProp(getConfig()), JsonObject.class);
      return essentialAttributes.stream().allMatch(p -> value.has(p));
    }
    return super.isValid(state);
  }

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  KafkaProperties(String config) {
    super(config);
  }

  public String getBootstrapServers(State state) {
    JsonObject value = get(state);
    if (value.has(BOOTSTRAP_SERVERS)) {
      return value.get(BOOTSTRAP_SERVERS).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getValueSerializer(State state) {
    JsonObject value = get(state);
    if (value.has(VALUE_SERIALIZER)) {
      return value.get(VALUE_SERIALIZER).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getKeySerializer(State state) {
    JsonObject value = get(state);
    if (value.has(KEY_SERIALIZER)) {
      return value.get(KEY_SERIALIZER).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getClientId(State state) {
    JsonObject value = get(state);
    if (value.has(CLIENT_ID)) {
      return value.get(CLIENT_ID).getAsString();
    }
    return DEFAULT_CLIENT_ID;
  }

  public String getTopicName(State state) {
    JsonObject value = get(state);
    if (value.has(TOPIC_NAME)) {
      return value.get(TOPIC_NAME).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getSchemaRegistryUrl(State state) {
    JsonObject value = get(state);
    if (value.has(SCHEMA_REGISTRY_URL)) {
      return value.get(SCHEMA_REGISTRY_URL).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getSchemaRegistryClass(State state) {
    JsonObject value = get(state);
    if (value.has(SCHEMA_REGISTRY_CLASS)) {
      return value.get(SCHEMA_REGISTRY_CLASS).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public void fillKafkaConfig(State state) {
    JsonObject value = get(state);
    for (Map.Entry<String, String> entry : kafkaConfigMapping.entrySet()) {
      if (StringUtils.isNotBlank(value.get(entry.getKey()).getAsString()) && value.has(entry.getKey())) {
        state.setProp(entry.getValue(), value.get(entry.getKey()).getAsString());
      }
    }
  }
}
