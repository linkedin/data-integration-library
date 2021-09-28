# Pagination

Pagination is typically used to fetch a large dataset from cloud over HTTP, where
one fetch can only optimally get a limited chunk of data. In such case,
the data is fetched through a series of pages. 

## Related

- [ms.pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.pagination.md),  
- [ms.session.key.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.session.key.field.md) 
- [ms.total.count.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.total.count.field.md)
- [asynchronous pattern](https://github.com/linkedin/data-integration-library/blob/master/docs/patterns/asynchronous-ingestion-pattern.md)

## Pagination Methods

There are different ways to control pagination. DIL supports the following
mechanisms.
  
### Method 1: Session Key

Pagination can be established through a session key or cursor. The data provider will
maintain the session key on the server side. The session key is exchanged between the 
server and the client in each request and each response. 

- APIs returning Json payload can embed the session key 
in the payload as a Json element or in the response header.

- APIs returning Csv payload can embed the session key 
in response header.

Session keys are designed by the data provider, and it comes in different forms. 

#### Request Id + Indicator

The session key can be as simple as a "hasMore" indicator, but that usually require
a client request id accompany each request. For example: 

The following will define a session key getting 
value from "hasMore" field of the response, and the pagination 
will stop once the condition is met. 

- `ms.session.key.field={"name": "hasMore", "condition": {"regexp": "false|False"}}`

#### Cursor

The session key can be dynamic and change in each response.
The following will define a session key getting value from the "records.cursor"   

- `ms.session.key.field={"name": "records.cursor"}`

The cursor is provided to the client in each response, and the client (DIL) 
will provide the same cursor in the subsequent request. 

### Method 2:  Record Position

Page Start is the starting row index, normally zero-based. APIs may 
call Page Start in different ways, commonly "offset", "page_start", or "start_record".

Page Size is the page size! It is normally a natural number. APIs
may call Page Size in different ways, commonly "limit", or "page_size". 
Page Size is normally fixed, and it won't change across pages, except 
that the last page may have a smaller size. A page size smaller than
the expected size is a signal of pagination ending.  

Page Number is normally a one-based page index. Again, APIs may call it 
in different ways. Page number generally increment by 1 in each page. 

#### Offset-limit Pair

The following pagination works through a offset-limit pair. 

`ms.pagination={"fields": ["offset", "limit"], "initialvalues": [0, 5000]}`

This will define a `pagestart` variable that is initialized as 0, and 
gets value from the "offset" field of the response. So every response 
will come back with different offset for next page, on and on, 
until an empty page is returned.
 
This will also define a `pagesize` variable that is initialized 
as 5000, and gets value from the "limit" field of the response. 
However, in most cases, the pagesize stays the same in almost all 
use cases.

When source of "offset" is not provided, DIL will calculate the
offset for next page assuming each page will be a full page. In the 
following example, only initial values are configured. 

- `ms.pagination={"initialvalues": [0, 5000]}`

This will define a continuous 5000-record page stream. The limit 
is 5000, and the offset will be accumulated, 0, 5000, 10000.... 
This will continue until other conditions are met to stop. 

#### Page Number

This can be an explicitly defined page number, if ms.pagination is defined. 

This can also be an implicitly defined page number, if ms.pagination 
is not defined. 

Page number can be sequentially increased, or can be retrieved 
from a field in the response

### Method 3: Total Record Count

This works when the response contains only a field indicating 
the total number of records. This field should be defined in 
`ms.total.count.field`. 

For example: `ms.total.count.field=totalResults` 

The total record count is not a variable in parameters. 
It is simply a value to control when pagination should stop.

## Pagination Stop Conditions
 
The pagination stops when one of following condition is met:

- An empty response 
- A blank session key
- The session key value met the stop condition
- The number of rows processed reached total row count
- Session timeout

## Pagination Availability by Protocol

- HTTP source supports all above methods
- JDBC source supports paging by record position.
- S3   source doesn't support pagination
- SFTP source doesn't support pagination
- HDFS source doesn't support pagination
 
## References

Session Key Pagination
- Gong.io API
- Zendesk API
- Eloqua API
- Salesforce bulk API

Record Positioning
- Qualaroo API
- RightNow JDBC 
- Eloqua API
- Zoom API

Total Record Count
- Gong.io API
- Zoom API

[Back to Summary](summary.md#pagination)

 