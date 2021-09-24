# ms.http.request.headers

**Tags**: [http](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/http-parameters.md)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:

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
