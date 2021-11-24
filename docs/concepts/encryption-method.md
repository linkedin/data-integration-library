# Encryption Methods

There are several types of encryption, and each can engage different methods. Encryption can be used in the following scenarios:

1. Encrypt credentials, like usernames, passwords, keys, or refresh tokens, in configurations
2. Encrypt confidential data, like access tokens, in data files
3. Encrypt credentials that are to be sent over network for authentication
4. Encrypt data files that are to be sent out to cloud storage

For each type of encryption, there is a need for corresponding decryption. 

## Static Encryption in Configuration

Secrets in configuration are encrypted using [Gobblin encryption utility](https://gobblin.readthedocs.io/en/latest/user-guide/Working-with-Job-Configuration-Files/#password-encryption).

- This encryption requires a master key that is stored in a location under `encrypt.key.loc` job property. 
- The encrypted secrets are enclosed within "ENC()".
- In runtime, DIL will call the encryption utility to decrypt the secretes enclosed within "ENC()" by pattern matching. 

Currently, encryption is a manual process, i.e., the pipeline developer need to manually encrypt the secrets before putting into the configuration. 

Decryption is automatically, but the detection of encrypted strings is limited to the following job properties.

- [](../parameters/ms.authentication.md)
- source.conn.username
- source.conn.password
- state.store.db.password (for MySQL state store only)

From example: 
- `ms.authentication={"method": "custom", "encryption": "", "header": "x-apikey", "token": "ENC(xxx)"}`
- `source.conn.username=ENC(xxx)`
- `source.conn.password=ENC(xxx)`
- `state.store.db.password=ENC(xxx)`

## On Storage Encryption

Confidential data can be encrypted. The encryption is at field level. This is an automatic process using Gobblin encryption utility API.

To encrypt a field before it is written to storage, include the field name in [ms.encryption.fields](../parameters/ms.encryption.fields.md).

For example: 
- `ms.encryption.fields=["access_token"]`

The field "access_token" will be encrypted on storage, and its value will be like "ENC(xxx)". The value can only be decrypted using the 
master key stored in a location under `encrypt.key.loc`. 

## Over the Network Secret Encryption

When sending credentials to data system for authentication, the secrets can be encrypted using encryption methods acceptable to the data system. 
Currently, DIL only supports "base64" encryption when sending username and password for authentication. Because username and password might contain
special characters, most data system using username/password, including those using user key and secret, require BASE64 encryption. 
See [ms.authentication](../parameters/ms.authentication.md)

*BASE64 is a reversible encryption, so it have to be sent over secure network to be safe*

For example: 

- `ms.authentication={"method": "custom", "encryption": "base64", "header": "x-apikey", "token":"ENC(xxx)"}`

In runtime, DIL will decrypt the "token" using Gobblin utility, and then encrypt it using BASE64 encryption.   

- `ms.authentication={"method": "basic", "encryption": "base64", "header": "Authorization"}`

In runtime, DILL will take credentials from `source.conn.username` and `source.conn.password`, decrypt them if encrypted, then
concatenate them to one string separated by ":", then encrypt the concatenated string using BASE64 encryption. 

## Over the Network Data Encryption

When ingesting data, the data systems could encrypt their data using GPG algorithm. Encrypted data can be decrypted using 
preprocessors, before being parsed by the extractor. 

### GPG Decryption

GPG Encryption/Decryption can be Symmetric or Asymmetric.

- Symmetric Decryption uses password only; no private key used.
- In Asymmetric Decryption, the source was encrypted using a public key, and optionally a password, the decryption need to use a private key, and the password if it was used.

DIL is able to decrypt a source stream if it uses one of the following GPG supported algorithms:
- 3DES
- IDEA (since versions 1.4.13 and 2.0.20)
- CAST5
- Blowfish
- Twofish
- AES-128, AES-192, AES-256
- Camellia-128, -192 and -256 (since versions 1.4.10 and 2.0.12)

The job configuration should use one of these ciphers to be accepted. And if no cipher is specified, CAST5 will be used.

See 
- [ms.extract.preprocessors](../parameters/ms.extract.preprocessors.md)
- [ms.extract.preprocessor.parameters](../parameters/ms.extract.preprocessors.md)

For example:
- `ms.extract.preprocessors=org.apache.gobblin.multistage.preprocessor.GpgEncryptProcessor`
- `ms.extract.preprocessor.parameters={"com.linkedin.cdi.preprocessor.GpgEncryptProcessor": {"keystore_path" :"/path/secret.gpg", "key_name": "999ABC", "keystore_password" : "ENC(password)"}}`

In above example, key_name is required for encryption, it is a long type id, and it should be formatted as an HEX string.

### GPG Encryption

Encryption accepts the same set of algorithms as decryption. Currently, encryption only works in FileDumpExtractor, and it will encrypt the whole file in once.

[Back to Summary](summary.md#encryption-methods)