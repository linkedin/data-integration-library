# ms.source.uri

**Tags**: 
[connection](categories.md#connection-properties),
[source](categories.md#source-properties)

**Type**: string

**Format**: URI with path segments and parameters

**Support DIL Variables**: Yes

## Required
Yes

**Default value**:
blank

## Related 

## Description 

ms.source.uri defines the integration point, which is called data source for data ingestion or target for data egression. 
It follows the URI format [here](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier). 
The only exception is that authority is not supported, because all authority cannot be fit in the URI.

ms.source.uri supports [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md) 
that allow substitution in runtime, for example:
`ms.source.uri = https://api.zendesk.com/api/v2/incremental/tickets.json?start_time={{startTime}}`

ms.source.uri requires protocol (scheme) and domain (host). Path segments and URI parameters are optional. 

## Examples

`
ms.source.uri=https://api.zendesk.com/api/v2/incremental/tickets.json?start_time={{startTime}}
ms.source.uri=jdbc://data.rightnow.com/marketing?useSSL=true
ms.source.uri=https://oauth2.googleapis.com/token
ms.source.uri=https://bucket-name.s3.amazonaws.com/{{s3key}}
`

[back to summary](summary.md#mssourceuri)
