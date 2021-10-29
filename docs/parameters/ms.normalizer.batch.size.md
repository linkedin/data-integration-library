# ms.normalizer.batch.size

**Tags**: 
[schema](categories.md#schema-properties),
[conversion](categories.md#conversion-properties)

**Type**: integer

**Default value**: 500

**Minimum value**: 1

**Related**:

- [job property: ms.target.schema](ms.target.schema.md)
- [job property: ms.target.schema.urn](ms.target.schema.urn.md)
- [job property: ms.source.schema.urn](ms.source.schema.urn.md)

## Description

`ms.normalizer.batch.size` specifies the batch size for the normalizer converter
to group rows. 

[NormalizerConverter](https://github.com/linkedin/data-integration-library/blob/master/docs/components/normalizer-converter.md),
including JsonNormalizerConverter and AvroNormalizerConvert, can group multiple columns into a single column, and group 
multiple rows to a single record. It compare the source schema with the target schema,
and put all columns in the source but not in the target into the first column that 
is in the target but not in the source.  

Assuming the source schema has A,**B,C,D** 4 columns, and the target has A, **X** 2 columns, 
and `ms.normalizer.batch.size=100`, then **B,C,D** are in the source but not in the target,
and **X** is in the target but not in the source. Therefore, for each record, B, C, D will be grouped
into a sub-record of a "map" type, and for each 100 records, the sub-records will
be formed into a new column "X" of "**array**" type.

Assuming the source schema has A,**B,C,D** 4 columns, and the target has A, **X** 2 columns, 
and **`ms.normalizer.batch.size=1`**, then **B,C,D** are in the source but not in the target,
and **X** is in the target but not in the source. Therefore, for each record, B, C, D will be grouped
into the new column "X" of "**map**" type.

Per above rule, setting `ms.normalizer.batch.size` to 1 has special effects of condensing a sparse
table. Assuming we have sparse table of 1 fixed column A, and 100s of sparse
columns (c1, c2 .... c300)
So some records may values in column A a few columns (c1, c2, ...), and other records may
have values in column A and a few other columns (c20, c21, ...). In such case,
the normalizer converter will put all sparse columns into a map field of key value
pairs, and only columns with values will be included in the map field. 

### Example: surveys

For example, surveys may have a few fixed columns, and many question columns. But each
survey may have a different set of questions. Therefore in order to store the surveys
in one table, one option would be putting all question-answser pairs into a 
response column. So the input data could be like:
- {"survey": "A", "q1":"...", "q2":"...", "q3":"...", ...}
- {"survey": "B", "qa":"...", "qb":"...", "qc":"...", ...}

With `ms.normalizer.batch.size=1`, and target schema like: {survey, responses}

The output data could be like
- {"survey": "A", "responses": {"q1":"...", "q2":"...", "q3":"...", ...}}
- {"survey": "B", "responses": {"qa":"...", "qb":"...", "qc":"...", ...}}
 
[back to summary](summary.md#msnormalizerbatchsize)