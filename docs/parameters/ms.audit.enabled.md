# ms.audit.enabled

**Tags**: 
[auditing](categories.md#auditing-properties)

**Type**: boolean

**Format**: true/false

**Default value**: false

## Related 
- [ms.kafka.brokers](ms.kafka.brokers.md)
- [ms.kafka.clientId](ms.kafka.clientId.md)
- [ms.kafka.schema.registry.url](ms.kafka.schema.registry.url.md)
- [ms.kafka.audit.topic.name](ms.kafka.audit.topic.name.md)

## Description 

Setting ms.audit.enabled to true will enable outgoing data auditing. Auditing will trace all outgoing data
including parameters and payloads to data lake through Kafka. 

Auditing is an important part of egression, but ingestion requests can also be audited.

[back to summary](summary.md#msauditenabled)
 