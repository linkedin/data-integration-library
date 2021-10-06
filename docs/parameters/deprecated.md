# Deprecated Job Properties

The following job properties are deprecated, some are no longer being effective, 
and some are replaced by new parameters. 

## ms.watermark.type

`ms.watermark.type` was designed as an DIL internal property. It is no longer being used. 

**Alert**: `ms.watermark.type` can cause parsing error if it is used in GaaS flow specs because
its prefix matches another effective parameter ms.watermark. Azkaban projects that still 
have this job property will have no harm. 

## ms.encoding

`ms.encoding` is replaced by [ms.source.data.character.set](ms.source.data.character.set.md).

## Factories

- ms.http.client.factory
- ms.source.schema.reader.factory
- ms.target.schema.reader.factory

Above factories are replaced by [ms.connection.client.factory](ms.connection.client.factory.md)

