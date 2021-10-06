# ms.http.request.method

**Tags**: 
[http](categories.md#http-properties)

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

[back to summary](summary.md#mshttprequestmethod)

