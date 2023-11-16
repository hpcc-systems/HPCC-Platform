#### getSecretKeyValue
    string getSecretKeyValue(secretId, key)

Lookup an *espUser* categorized secret based on a secret identifier, and extract the property value identified by `key`. Because this function enables exposure of all data in the named secret, only secrets defined in the *espUser* category can be accessed with this function.

| Parameter | Required? | Description |
| :- | :-: | :- |
| key | Y | An identifier of a possible secret property value. |
| secretId | Y | The identity of a secret, expressed as `[ vaultId "::" ] secretName [ "::" version ]`. A `secretName` is required always, and a `vaultId` is required before a version can be given. |
