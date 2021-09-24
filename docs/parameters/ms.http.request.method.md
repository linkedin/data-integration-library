# ms.http.request.method

**Tags**: [http](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/http-parameters.md)

**Type**: string

**Format**: one of the values of get, put, post, and delete

**Default value**: blank (equivalent to GET)

## Related 

## Description 

The expected HTTP method to send the requests, decided by the API.

- GET: all parameters are specified in the URL as URL parameters
- POST: parameters are sent to API in request body
- PUT: parameters are sent to API in request body
- DELETE: parameters are sent to API in request body

**Note**: URL parameters are URL encoded.

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mshttprequestmethod)

