# ms.csv.default.field.type

**Tags**: 
[csv extractor](categories.md#csv-extractor-properties)

**Type**: string

**Default value**: blank

## Related 
- [ms.csv.column.header](ms.csv.column.header.md)
- [ms.csv.column.header.index](ms.csv.column.header.index.md)
- [ms.csv.column.projection](ms.csv.column.projection.md)
- [ms.csv.escape.character](ms.csv.escape.character.md)
- [ms.csv.quote.character]()
- [ms.csv.separator]()
- [ms.csv.skip.lines](ms.csv.skip.lines.md)

## Description

ms.csv.default.field.type specifies a default type to supersede field type inference.
 
By default, CsvExtractor tries to infer the true type of fields when inferring schema
However, in some cases, the inference is not accurate, and users may prefer to keep all fields as strings.
In this case `ms.csv.default.field.type = string`

Supported types: string | int | long | double | boolean | float

[back to summary](summary.md#mscsvdefaultfieldtype)
