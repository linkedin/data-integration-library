# ms.derived.fields

**Tags**:
[conversion](categories.md#conversion-properties)

**Type**: string

**Format**: A JsonArray of JsonObjects

**Default value**:blank

## Related 

## Description 

Derived Fields are calculated fields that serve critical roles in data ingestion process, such as compaction. This includes, but is not
limited by, the following:

- Convert a date string to a EPOC date value so that downstream compaction/partitioning can use the long EPOC value
- Extract a part of a field to form a primary key or delta key
- Provide a calculated value based on flow execution state or watermark, such as the often used extraction date derived field
- Lift up a nested element in the response to the toplevel and make it a toplevel field because only toplevel fields can be primary keys or delta keys
- Persist a job execution variable, such as the work unit identifier, into the final dataset 

ms.derived.fields can have an array of derived field definitions with each derived field definition being a JsonObject.

Each derived field definition will have a "name" and a "formula". The "name" specifies the eventual field name of the derived field
in the output dataset, and the "formula" specifies how to make the derived field. 
 
The formula can be specified by "type", "source", and "format". 

DIL supports 6 types of derived fields:

- `epoc`: it provides a time value field in the form of epoc (millisecond level). 
- `regexp`: it provides a string value field by extracting the value from another field using a Java REGEXP pattern
- `string`: it provides a string value field by taking from another field, which can be a nested field, without transformation
- `integer`: it provides a integer value field by taking from another integer field, which can be a nested field, without transformation
- `number`: it provides a number value field by taking from another number field, which can be a nested field, without transformation
- `boolean`: it provides a boolean value field by taking from another boolean field, which can be a nested field, without transformation

DIL supports 3 sources of derivation:
- from another field in the payload, in this case, the source can be the field name, or a JsonPath that leads to the field
- from a dynamic DIL [variable](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md)
- from predefined DIL symbols, which can be CURRENTDATE, or PxD (see [date interval](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/iso-date-interval.md))

When the type is `epoc`, a format is required to specify how to convert the source to the desired EPOC value.  

### Example 1: the following defines a derived field using regular expression to subtract part of a source field </p>
`[{
  "name": "surveyid",
  "formula": {
    "type": "regexp",
    "source": "survey_url",
    "format": "https.*\\/surveys\\/([0-9]+)$"
    }
}]`

### Example 2: the following defines an epoc timestamp field to meet compaction requirement </p>
`[{
  "name": "callDate",
  "formula": {
    "type": "epoc",
    "source": "started",
    "format": "yyyy-MM-dd"
  }
}]`

### Example 3: the following defines derived fields from variables that are generated from secondary input. 
 
Assuming the secondary input provides an id value that we would like to make it into the final dataset as a
permanent field. 
`ms.secondary.input=[{"path": "/path/to/hdfs/inputFileDir/2019/08/07/19/", "fields": ["id", "tempId"]}]` </p>

Then the derived field can be configured as:

`[{"name": "id", "formula": {"type": "string", "source": "{{id}}"}}]`

### Example 4: the following defines a derived field from pagination control variables. 

Assuming the pagination is controlled by page number and page size, and we would like to persist the page size in the 
final dataset.
 
`ms.parameters=[{"name": "pageNo", "type": "pageno"},{"name": "size", "type": "pagesize"}]`

Then the derived field can be defined as:

`[{"name": "pagesize", "formula": {"type": "integer", "source": "{{pagesize}}"}}]`

### Example 5: the following defines an epoc timestamp field based on flow execution time </p>
`[{"name": "extractedDate", "formula": {"type": "epoc", "source": "CURRENTDATE"}}]`


[back to summary](summary.md#msderivedfields)
