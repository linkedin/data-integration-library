# ms.audit.enabled

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category
[auditing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/auditing-parameters.md)

## Type
boolean

## Format
true/false

## Required
No

## Default value
false

## Related 

## Description 

Setting ms.audit.enabled to true will enable outgoing data auditing. Auditing will trace all outgoing data
including parameters and payloads to data lake through Kafka. 

Auditing is an important part of egression, but ingestion requests can also be audited. 