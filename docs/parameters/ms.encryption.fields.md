# ms.encryption.fields

**Tags**: 
[conversion](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#conversion-properties)

**Type**: string

**Format**: JsonArray

**Default value**: blank

**Related**:

## Description

`ms.encryption.fields` specifies a list of fields to be encrypted before
they are passed to converters. 

In this job property you can specify the fields (array of fields) 
that needs to be encrypted by the Gobblin Encryption utility.

These fields can be of JsonPrimitive type (string/int/boolean/etc.) or 
JsonObject type (with nested structure). For the later, the field 
name can be Json Path.

### Example

`ms.encryption.fields=["access_token", "client_secret", "refresh_token"]`
`ms.encryption.fields=["emailAddress", "settings.publicKey"]`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#msencryptionfields)

