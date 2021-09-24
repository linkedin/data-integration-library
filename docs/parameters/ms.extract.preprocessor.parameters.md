# ms.extract.preprocessor.parameters

**Tags**: 

**Type**: string

**Format**: A JsonObject

**Related**:
- [job property: ms.extract.preprocessors](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extract.preprocessors.md)

## Description

When a source file is encrypted, it requires credentials to decrypt.
`ms.extract.preprocessor.parameters` defines parameters to pass into the 
preprocessor along with the input. 

For GPG based decryption/encryption, parameters needed are: 
- "**action**" : string, decrypt/encrypt
- "**keystore_password**" : string, some password,
- "**keystore_path**" : string, path to the secret keyring,
- "**cipher**" : string, optional, cipher algorithm to use, default to CAST5 (128 bit key, as per RFC 2144)
- "**key_name**" : string, optional, the key id, a long value, of the public Gpg key as a Hex string

### Example

The following provides key and password to GPG decryption:
- `ms.extract.preprocessor.parameters={"com.linkedin.cdi.preprocessor.GpgDecryptProcessor": {"keystore_path" :"/some path/secret.gpg", "keystore_password" : "ENC(some password)"}}`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#msextractpreprocessorparameters)
