# Deprecated Job Properties

The following job properties are deprecated, some are no longer being effective, 
and some are replaced by new parameters. 

## ms.watermark.type

`ms.watermark.type` was designed as an DIL internal property. It is no longer being used. 

**Alert**: `ms.watermark.type` can cause parsing error if it is used in GaaS flow specs because
its prefix matches another effective parameter ms.watermark. Azkaban projects that still 
have this job property will have no harm. 

## ms.http.client.factory

`ms.http.client.factory `and other client factories are replaced 
by [ms.connection.client.factory](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.connection.client.factory.md)

ms.http.status.reasons

## ms.encoding

`ms.encoding` is replaced by [ms.source.data.character.set](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.data.character.set.md).