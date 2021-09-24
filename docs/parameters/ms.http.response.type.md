# ms.http.response.type

**Tags**: [http](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/http-parameters.md)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:

## Description

`ms.http.response.type` specifies less common response types in addition to
 the default ones "application/json" or "text/csv". 
  
If you have a custom response type other than "application/json" or "text/csv", 
you can configure expected response using this parameter  you can configure expected response using this parameter

When `ms.http.response.type` is not configured, the default 
for CsvExtractor is "text/csv", and for JsonExtractor it is "application/json".

### Example

The following accepts "application/x-gzip" in addition to "text/csv" 
in a CsvExtractor
- `ms.http.response.type={"Content-Type":"application/x-gzip"}`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mshttpresponsetype)
