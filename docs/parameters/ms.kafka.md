# ms.kafka

**Tags**: 
[monitoring](categories.md#monitoring-properties),

**Type**: string

**Format**: JsonObject

**Default value**: {} (blank JsonObject)

## Related 

## Description 

`ms.kafka` defines essential Kafka producer configs with ssl. 

`ms.kafka` comes as a JsonObject, and it can have any of the following
attributes:     

- **bootstrapServers**, specifies the list of kafka brokers you are trying to connect to. This is a mandatory property to use kafka for reporting any events.
- **valueSerializer**, specifies the name of the serializer class for the value that implements `org.apache.kafka.common.serialization.Serializer` interface
- **keySerializer**, specifies the name of the serializer class for the key that implements `org.apache.kafka.common.serialization.Serializer` interface
- **topicName**, specifies the name of the Kafka topic the event is emitted to.
- **schemaRegistryClass**, is an optional parameter that specifics the name of schema registry class in case you host the data contract for kafka in a specific schema registry
- **schemaRegistryUrl**, is an optional parameter that specifies the url of schema registry, typically schema registry itself can be a service that should be able to support REST API

SSL attributes to connect to Kafka 
- **securityProtocol**, specifies the type of SSL supported by default we set SSL protocol
- **keyStoreType**, specifies keystore type default is `"pkcs12"`
- **keyStorePath**, specifies file path to key store file
- **keyStorePassword**, specifies the key to decrypt key store file
- **keyPassword**, specifies the password to decrypt the key
- **trustStorePath**, specifies the file path to trust store
- **trustStorePassword**, specifies the key to decrypt trust store
- **connectionTimeoutSeconds**, wait time for establishing a connection

**Example**: 
```
{
    "bootstrapServers": "kafka.corp.com:1000",
    "valueSerializer": "org.apache.kafka.common.serialization.Serializer",
    "keySerializer": "org.apache.kafka.common.serialization.Serializer",
    "schemaRegistryUrl": "http://schemaregistry.corp.com:1001/schemaRegistry/schemas",
    "schemaRegistryClass": "org.apache.gobblin.kafka.schemareg.LiKafkaSchemaRegistry",
    "topicName": "CdiTrackingEvent",
    "keyStoreType": "pkcs12",
    "keyStorePath": "/User/test/identity.p12",
    "keyStorePassword": "test_pass",
    "keyPassword": "test_pass",
    "trustStorePath": "/User/test/certs",
    "trustStorePassword": "test_pass",
    "securityProtocol": "SSL"
}
```

Besides, if you need to pass additional Kafka producer related properties they can be passed as regular key value pairs in your job file. For more additional properties please refer to `https://kafka.apache.org/documentation/#producerconfigs`.

[back to summary](summary.md#mskafkamskafka)
