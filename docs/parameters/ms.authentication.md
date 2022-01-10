# ms.authentication

**Tags**: 
[authentication](categories.md#authentication-properties)
[http](categories.md#http-properties)

**Type**: string

**Format**: JsonObject

**Default value**: blank

**Accepts Variable Substitution**: Yes

## Related 
- [ms.http.request.headers](ms.http.request.headers.md)
- [ms.secondary.input](ms.secondary.input.md)
- [ms.parameters](ms.parameters.md)
- source.conn.username
- source.conn.password

## Description 

ms.authentication job property defines the authentication of a request. It works with HTTP protocol only 
for now, but could be used in other protocols. 

ms.authentication is designed to be an JsonObject with the following fields:

- **method**: `method` field specifies the authentication scheme, it is a string allowing these 
values: basic|bearer|oauth|custom. see [authentication method](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/authentication-method.md).

- **encryption**:`encryption` field specifies how to encrypt the credentials, it is a string allowing
`base64` or `none`. see [encryption method](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/encryption-method.md).

- **header**: `header` field specifies what HTTP request header tag to associate the credential withs. In most
cases, this is `Authorization`.

- **token**: `token` field specifies the values like access token, bearer token, or refresh token etc. If
not specified, a token is made from the concatenation of the values of source.conn.username 
and source.conn.password. Tokens are considered secrets, and job configuration should have its encrypted 
value. see [secret encryption](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/secret-encryption.md).
Encrypted tokens are decrypted, and encrypted with the `encryption` algorithm, before being used in requests.  

## Example 1: Base Authentication

With base authentication, source.conn.username and source.conn.password specifies user name and password, or
user key and secret key. Then user name and password are concatenated with `:` as separator. The combined
string is then encrypted using base64 algorithm. 

A typical configuration would be like this: 

`ms.authentication = {"method": "bearer", "encryption": "base64", "header": "Authorization"}`

The combined user name and password string can also be supplied through the `token` field for cleaner
configuration. This avoids using source.conn.username and source.conn.password job properties. 

`ms.authentication={"method": "bearer", "encryption": "base64", "header": "Authorization", "token": "xxxx"}`

## Example 2: Bearer Token Authentication

Bearer token can be specified directly: 

`ms.authentication={"method": "bearer", "encryption": "none", "header": "Authorization", "token": "xxxx"}`

or as a variable:

`ms.authentication={"method": "bearer", "encryption": "none", "header": "Authorization", "token": "{{access_token}}"}`

The typical application of the second way is OAuth2 where the access token need to be refreshed ahead of
every job execution, and then the updated access token can be read in through secondary input. When the
access token is read in through secondary input, it is stored in a variable. And that variable can then
be referenced in the `token` field. 

## Example 3: Custom Token Authentication

The following configuration works for an API that requires "x-apikey" header, instead of "Authorization" header. 
The token should be provided directly without encryption.

`ms.authentication={"method": "custom", "encryption": "none", "header": "x-apikey", "token": "xxxx"}`

[back to summary](summary.md#msauthentication)