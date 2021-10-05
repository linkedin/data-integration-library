# ms.data.default.type

**Tags**: 
[schema](categories.md#schema-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: blank

**Related**:

## Description

`ms.data.default.type` provides a way to explicitly specifies data 
types for certain fields. This is necessary when the source data has 
empty fields, like placeholders, and DIL cannot infer its type properly.

### Example

In the following example, 2 fields are placeholders, and they don't have any 
value. We can explicitly put a type for them. 

- `ms.data.default.type={"extension": "string", "personalMeetingUrls": "string"}`

[back to summary](summary.md#msdatadefaulttype)

