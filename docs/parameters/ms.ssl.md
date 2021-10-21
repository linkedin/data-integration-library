# ms.ssl

**Tags**: 
[connection](categories.md#connection-properties),

**Type**: string

**Format**: JsonObject

**Default value**: {} (blank JsonObject)

## Related 

## Description 

`ms.ssl` defines SSL parameters. 

`ms.ssl` comes as a JsonObject, and it can have any of the following
attributes:     
- keyStoreType, the key store type, default is "pkcs12"
- keyStorePath, the file path to key store file
- keyStorePassword, the key to decrypt the key store file
- keyPassword, the password to decrypt the key
- trustStorePath, the file with trust certificate
- trustStorePassword, the password to decrypt the trust store file
- connectionTimeoutSeconds, the wait time to establish a connection, the default is 60 seconds
- socketTimeoutSeconds, the wait time for the next packet, the default is 60 seconds
- version, the SSL version, default is "TLSv1.2" 

[back to summary](summary.md#msssl)
