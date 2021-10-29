# ms.http.conn.ttl.seconds

**Tags**: 
[http](categories.md#http-properties)

**Type**: Integer

**Default value**: 10

**Related**:
- [ms.http.conn.max](ms.http.conn.max.md)
- [ms.http.conn.per.route.max](ms.http.conn.per.route.max.md)


## Description

`ms.http.conn.ttl.seconds` defines maximum idle time allowed when there
is no activity on an HTTP connection. When there is no activity after
TTL passed, the connection is disconnected. 

[back to summary](summary.md#mshttpconnmax)
