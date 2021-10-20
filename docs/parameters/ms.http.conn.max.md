# ms.http.conn.max

**Tags**: 
[http](categories.md#http-properties)

**Type**: Integer

**Default value**: 50, 0 will be translated to the default value

**Maximum value**: 500

**Related**:
- [ms.http.conn.per.route.max](ms.http.conn.per.route.max.md)


## Description

`ms.http.conn.max` defines maximum number of connections to keep
in a connection pool. It limits the total connections to an HTTP
server. 

In a MAPREDUCE mode, because each mapper runs on a separate container,
this parameter can only limit the total connections from each container;
therefore, the total connections to HTTP server, from all mappers, 
can be more than this value.   

In actual execution, DIL closes the HTTP connection once a data stream
is processed; hence we should not expect very high number of connections
to the server.  

[back to summary](summary.md#mshttpconnmax)
