# ms.hdfs.reader.parse.json.strings

**Tags**:

**Type**: boolean

**Format**: true/false

**Default value**: false

## Related
- [ms.secondary.input](ms.secondary.input.md)

## Description

When set to `true`, `HdfsReader` detects string-typed fields whose values parse as valid JSON (objects or
arrays) and inlines them as JSON elements in the outgoing payload, rather than escaping them as JSON string
primitives.

This is useful when a pre-serialized JSON document (including JSON-LD, which uses `@`-prefixed field names
that Avro schemas cannot express) is stored as an Avro `string` field and needs to be sent inline in an HTTP
POST body.

### Behavior

- `false` (default): string fields are always serialized as JSON string primitives. This is the existing
  behavior and is preserved for backward compatibility.
- `true`: for each string field, if the trimmed value begins with `{`/`[` and ends with `}`/`]` and parses
  as valid JSON, the parsed JSON element replaces the string primitive. Parse failures fall back to the
  default string-primitive behavior.

### Example

Given an Avro record with `data: string` containing `[{"@context":"https://schema.org"}]`:

- Flag off: `{"data":"[{\"@context\":\"https://schema.org\"}]"}` (escaped)
- Flag on: `{"data":[{"@context":"https://schema.org"}]}` (inlined)
