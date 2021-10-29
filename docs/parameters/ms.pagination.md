# ms.pagination

**Tags**: 
[pagination](categories.md#pagination-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:
- [key concept: pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md)
- [ms.session.key.field](ms.session.key.field.md)
- [ms.total.count.field](ms.total.count.field.md)

## Description

`ms.pagination` defines key pagination attributes. 
`ms.pagination` is a JsonObject with 2 members:

- **fields**: is an array of up to 3 string elements, each denote a source key column for:
  1. page start, or offset
  2. page size, or limit of each page
  3. page no, if page no is used to control instead of using page start and page size
- **initialvalues**: is an array of up to 3 integers, each denote an initial value for:
  1. page start, or offset
  2. pagesize, or limit of each page
  3. page no, if page no is used to control instead of using page start and page size

The field names defined by `fields` are element names in the response
from data source. 

`fields` are optional. If not defined, but initial offset and page size are given, DIL will automatically
increase page number by 1 and increase offset by page size. DIL will auto-increase
page number by 1 for each page whenever pagination is enabled.  

Pagination is enabled whenever `fields` or `initialvalues` are provided. 
If none of them are provided, pagination is disabled. 

Variables can be defined using the special pagination types. 
See [ms.parameters](ms.parameters.md).

### Example 1

Say your source doesn't provide offset, but provides the size of 
each page under key "page_size", and the number of total pages under 
key "page_number". You can set page start to 0 since offset is not 
being used. And you can set page_size to be the value you want, and 
set page_number to 1. The page_number will auto-increment by 1 with 
each subsequent request sent to the data source.

The job configuration is:

- Specify that we will read "page_size" and "page_number" from the response
`ms.pagination={"fields": ["", "page_size", "page_number"], "initialvalues": [0, 100, 1]}`
- Define variables `size` and `pageNo`
`ms.parameter=[{"name": "pageNo", "type": "pageno"},{"name": "size", "type": "pagesize"}]`
- Specify that we will use variable `size` and `pageNo` in URL
`ms.source.uri=https://api.abc.com/q?page_size={{size}}&page_number={{pageNo}}`

The first request URL becomes:
`https://api.abc.com/q?page_size=100&page_number=1`

The second request URL becomes:
`https://api.abc.com/q?page_size=100&page_number=2`

### Example 2

Say your source doesn't provide page numbers, but accepts pagination 
by updating the offset. 

The job configuration is:
- Specify that we will read "offset" and "limit" from the response
`ms.pagination={"fields": ["offset", "limit"], "initialvalues": [0, 5000]}`
- Define variables `offset` and `limit`
`ms.parameter=[{"name": "offset", "type": "pagestart"}, {"name": "limit", "type": "pagesize"}]`
- Specify that we will use variables `offset` and `limit` in URL
`ms.source.uri=https://api.abc.com/q?offset={{offset}}&limit={{limit}}`

And the first request URL becomes:
`https://api.abc.com/q?offset=0&limit=5000`

And the second request will auto increment the offset, and the URL becomes:
`https://api.abc.com/q?offset=5000&limit=5000`
  
[back to summary](summary.md#mspagination)