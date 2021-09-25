# ms.http.request.headers

**Tags**: 
[http](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#http-properties),
[authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/authentication-properties.md)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:
- [ms.authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.authentication.md)
- [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.secondary.input.md)
- [ms.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.parameters.md)
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

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mshttprequestheaders)
