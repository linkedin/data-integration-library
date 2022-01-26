# ms.secret.manager.class

**Tags**: 

**Type**: string

**Default value**: `com.linkedin.cdi.util.GobblinSecretManager`

**Related**:

## Description

`ms.secret.manager.class` specifies the SecretManager class to use for secrets encryption and decryption.

Secrets include usernames, passwords, API keys, tokens, etc, that are essential for connections to other
data systems. 

Currently, we have the following SecretManager:

- `com.linkedin.cdi.util.GobblinSecretManager`

[back to summary](summary.md#mssecretmanagerclass)
