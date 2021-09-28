# ms.http.request.headers

**Tags**: 
[http](categories.md#http-properties),
[authentication](categories.md#authentication-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:
- [ms.authentication](ms.authentication.md)
- [ms.secondary.input](ms.secondary.input.md)
- [ms.parameters](ms.parameters.md)
- source.conn.username
- source.conn.password

## Description

`ms.http.request.headers` specifies custom headers including Content-Type that are to be 
included in HTTP requests. 

### Examples

The following define request content type:

- `ms.http.request.headers={"Content-Type": "application/json"}`
- `ms.http.request.headers={"Content-Type": "application/x-www-form-urlencoded"}`

The following defines request content type and other headers:

- `ms.http.request.headers={"Content-Type": "application/json", "developer-token": "...", "login-customer-id":"..."}`

[back to summary](summary.md#mshttprequestheaders)
