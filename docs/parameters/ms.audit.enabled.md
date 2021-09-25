# ms.audit.enabled

**Tags**: 
[auditing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#auditing-properties)

**Type**: boolean

**Format**: true/false

**Default value**: false

## Related 
- [ms.kafka.brokers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.brokers.md)
- [ms.kafka.clientId](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.clientId.md)
- [ms.kafka.schema.registry.url](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.schema.registry.url.md)
- [ms.kafka.audit.topic.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.audit.topic.name.md)

## Description 

Setting ms.audit.enabled to true will enable outgoing data auditing. Auditing will trace all outgoing data
including parameters and payloads to data lake through Kafka. 

Auditing is an important part of egression, but ingestion requests can also be audited.

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#msauditenabled)
 