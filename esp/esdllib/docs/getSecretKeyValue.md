#### getSecretKeyValue
    string getSecretKeyValue(key, secretName)
    string getSecretKeyValue(key, secretName, vaultId)
    string getSecretKeyValue(key, secretName, vaultId, version)

Lookup an *espUser* categorized secret based on a combination of name, vault ID, and version, and extract the property value identified by `key`. Because this function enables exposure of all data in the named secret, only secrets defined in the *espUser* category can be accessed with this function.

| Parameter | Required? | Description |
| :- | :-: | :- |
| key | Y | An identifier of a possible secret property value. |
| secretName | Y | The name of a potential secret. |
| vaultId | N | An identifier of the repository presumed to hold the named secret. |
| version | N | The requested version of the named secret. |
