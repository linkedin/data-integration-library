# ms.source.data.character.set

**Tags**:
 [source](categories.md#source-properties)

**Type**: string

**Format**: one of the standard codes in Java:StandardCharsets

**Default value**: "UTF-8"

**Related**:

## Description

`ms.source.data.character.set` specifies a character set to parse JSON or CSV payload. 
The default source data character set is UTF-8, which should be good for most use cases.

See `Java:StandardCharsets` for other common names, such as UTF-16.  

### Example

`ms.source.data.character.set=UTF-8`

[back to summary](summary.md#mssourcedatacharacterset)