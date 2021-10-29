# ms.csv

**Tags**: 
[extractor](categories.md#extractor-properties),

**Type**: string

**Format**: JsonObject

**Default value**: {} (blank JsonObject)

## Related 

## Description 

`ms.csv` defines csv extraction and conversion parameters. 

`ms.csv` comes as a JsonObject, and it can have any of the following
attributes:     

- **linesToSkip**, specifies how many lines of data to skip in the CSV payload.
The linesToSkip need to be more than the columnHeaderIndex. 
For example, if columnHeaderIndex = 0, the number of lines to skip need to be at least 1. 
When the linesToSkip is not set explicitly, and the columnHeaderIndex is set, linesToSkip = columnHeaderIndex + 1.
When neither linesToSkip and columnHeaderIndex are set, linesToSkip = 0.  
If more lines need to be skipped after the header, then set this parameter explicitly.
- **columnHeaderIndex**, specifies the 0-based row index of the header columns if they are available.
The valid value range is [-1, Integer.MAX_VALUE). The default value is -1, which means no header row.  
CSV files may have 1 or more descriptive lines before the actual data. These descriptive lines, 
including the column header line, should be skipped. 
Note the column header line can be in any place of the skipped lines. 
- **escapeCharacter**, specifies how characters can be escaped. Default is "u005C" (backslash \). 
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: \ can be specified as "u005c".
- **quoteCharacter**, specifies how source data are enclosed by columns. Default is double-quote (").
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: | can be specified as "u007C".
- **fieldSeparator**, specifies the field delimiter in the source csv data. The default is comma.
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: tab (\t) can be specified as "u0009".
- **recordSeparator**, also called line separator, specifies the line or record
delimiter. The default is system line separator. 
This can be specified as a variation of unicode without a backslash (\) before 'u'.
- **columnProjection**, defines how CSV columns should be arranged and filtered after parse,
before being sent to converter and writer to persist. 
This feature is primarily used to extract selected columns from csv source without a header.
Column projection definition is a comma-separated string, where each value is either an 
integer or a range, with each number representing the 0 based index of the field.
Column projection definition is inclusive, i.e., only the selected fields are included
in the final dataset, if a column projection is defined.  
For example, to include the 0th, 2nd, 3rd, and 4th column from a source that has 6 columns, 
set the value to: `"columnProjection": "0,2-4"`
- **defaultFieldType**, specifies a default type to supersede field type inference.
By default, CsvExtractor tries to infer the true type of fields when inferring schema
However, in some cases, the inference is not accurate, and users may prefer to keep all fields as strings.
In this case `"defaultFieldType": "string"`. 
Supported types: string | int | long | double | boolean | float.
- **maxFailures**, this is for the future CSV converter.
- **keepNullString**, this is for the future CSV converter.

See [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

[back to summary](summary.md#mscsv)
