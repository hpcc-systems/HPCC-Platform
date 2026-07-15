# Test Auth Security Manager

`testauthSecurity` is a lightweight, file-configured security plugin intended exclusively
for **development and testing**.  It lets you exercise ESP authentication and authorisation
code paths—including auth-failure handling—without requiring a live LDAP/Active Directory
server or any external identity provider.

Capabilities:

* Username/password authentication driven entirely by static configuration.
* Passwords default to the username, but can be provided as either encrypted attributes in the configuration or as the `password` value in a configured `@secretKey` name.
* Fine-grained, per-user authorisation: feature resources, file scopes, and ECL workunit
  scopes.
* Scripted authentication-state injection: configure a user entry to fail authentication
  with a specific `authStatus` value (e.g. `AS_ACCOUNT_LOCKED`, `AS_PASSWORD_EXPIRED`)
  so that the corresponding ESP failure-handling paths are exercised automatically.
* Optional password-expiration date per user so that code paths using
  `ISecUser::getPasswordDaysRemaining()` can also be exercised.

> Note: The plugin may not support every capability that an LDAP-configured system would. Open a ticket to add any missing features that would be useful for testing.

## Containerized Pre-Configured Sample

The containerized platform install includes a canned `testauthSecurity` configuration that you can enable by setting your service's auth type. This is a quick and easy solution if you need testing with security enabled and the two pre-set users are sufficient.

To enable it, set  `auth: test` in your esp application's values.yaml override like this:

``` yaml
esp:
- name: eclwatch
  application: eclwatch
  auth: test
```

This configures two users:

1. **testadmin** : Defaults to full access of file scopes, ECL WU scopes and resources.
2. **testuser** : Full resource access to WsSMC, WsWorkunits and WsTopology and full access to only _testuser_ created file scopes and ECL WU scopes.

Refer to the full canned configuration at [esp/applications/eclwatch/testauth_authorization_map.yaml](../esp/applications/eclwatch/testauth_authorization_map.yaml).

## Configuration Examples

This plugin is not supported by the configmgr, so for non-containerized deployments you must modify an environment.xml file. To generate the esp.xml from the environment.xml to use in debugging, you can use the `init.d/hpcc-init -c <myesp> setup` command, or systemd equivalent `systemctl start hpcc-conf@<myesp>.service`.

For containerized deployments you just need to modify the esp component yaml configuration as described below in the [Containerized section](#containerized-valuesyaml).

### Bare Metal (environment.xml)

Configuration requires two parts:

**1. Define the SecurityManager in `/Environment/Software`:**
```xml
<Software>
  <TestAuthSecMgr
    name="testauthsecmgr"
    type="SecurityManager"
    libName="libtestauthSecurity.so"
    instanceFactoryName="createInstance">
    <!-- Admin user: full access to all features -->
    <userAccess userName="admin" password="password">
      <defaults resource="Full" fileScope="Full" eclWUScope="Full"/>
    </userAccess>

    <!-- User with explicit, non-default access:
         - explicit Full grants for all WsSMC feature resources
         - explicit WsWorkunits feature grants
         - workunit scope attribute names are the submitting user's login name (lowercase) -->
    <userAccess userName="testuser" password="password">
      <defaults resource="None" fileScope="None" eclWUScope="None"/>
      <resources>
        <WsSMC>
          <features SmcAccess="Full"
                    ThorQueueAccess="Full"
                    MachineInfoAccess="Full"
                    ClusterTopologyAccess="Full"/>
        </WsSMC>
        <WsWorkunits>
          <features OwnWorkunitsAccess="Full"/>
        </WsWorkunits>
      </resources>
      <!-- grants Full access to all files whose scope is exactly 'testuser',
           i.e. files with logical names of the form 'testuser::<anything>' -->
      <fileScopes>
        <fileScope name="testuser" access="Full"/>
        <!-- example of a more-specific nested scope (overrides the parent if needed): -->
        <!-- <fileScope name="testuser::readonly" access="Read"/> -->
      </fileScopes>
      <!-- grants Full access to all workunits whose scope is 'testuser',
           i.e. workunits submitted by the user whose login name is 'testuser' -->
      <eclWUScopes testuser="Full"/>
    </userAccess>

    <!-- User whose password is valid but expired –
         ESP will redirect to the update-password page -->
    <userAccess userName="pwexpired" password="password"
                authenticateStatus="AS_PASSWORD_VALID_BUT_EXPIRED"
                passwordExpiration="2024-01-01">
      <defaults resource="Full" fileScope="Full" eclWUScope="Full"/>
    </userAccess>
  </TestAuthSecMgr>
</Software>
```
Required attributes on `<TestAuthSecMgr>`:

`name` — instance identifier (referenced by EspBindings)
`type="SecurityManager"` — marks it as a security manager plugin
`libName` — the compiled library (.so file)
`instanceFactoryName` — factory function in the library (usually createInstance)

**2. Reference it in `/Environment/Software/EspProcess`:**
```xml
<EspProcess name="myesp">
  <Authentication ... method="secmgrPlugin"/>
  <EspBinding ... type="testauthsecmgr">
    ...
  </EspBinding>
</EspProcess>
```
The EspProcess element must be modified:

- Each service binding that uses this security manager has `EspBinding/@type` matching `TestAuthSecMgr/@name`, in this example `EspBinding/@type="testauthsecmgr"` .
- Set `Authentication/@method="secmgrPlugin"`

The XSLT transformation then extracts the manager definition and merges it into the generated component XML.

### Containerized (values.yaml)

In a Helm-based HPCC deployment, the security manager is configured under the `esp`
section of `values.yaml` (or an override values file).  The plugin settings must be
nested under `authNZ.<auth-name>`, where `<auth-name>` matches the `auth:` attribute
value.

`useResourceMapsFrom: ldap` is a shortcut to re-use the same resource maps predefined
in [esp/applications/eclwatch/ldap_authorization_map.yaml](../esp/applications/eclwatch/ldap_authorization_map.yaml) for the ldap security plugin. Using this shortcut is recommended
unless you're testing new resource maps.

The secmgr plugin is identified and selected by `LibName` value.

> Note: The `name` and `type` fields are required, but the values aren't significant;
`name` becomes the `Authenticate` method, and `type` names the subtree passed to the
plugin as `secMgrCfg`.

```yaml
esp:
  - name: eclwatch
    auth: testauthSecurity
    authNZ:
      # Key must match the auth: value above
      testauthSecurity:
        # name and type are required for internal use, but the values aren't significant
        name: testauthSecurity
        type: testauthSecurity
        useResourceMapsFrom: ldap
        LibName: libtestauthSecurity.so
        userAccess:
          # Normal user with full access
          - userName: admin
            password: password
            defaults:
              resource: Full
              fileScope: Full
              eclWUScope: Full

          # User with explicit, non-default access:
          # - explicit Full grants for all WsSMC feature resources
          # - explicit WsWorkunits feature grants
          # - File scope keys are the logical-file scope string: everything before the last
          #   '::' in the logical filename.  Examples:
          #     file 'testuser::myfile'        → scope key 'testuser'
          #     file 'testuser::data::myfile'  → scope key 'testuser::data'
          #   Hierarchical lookup means a key of 'testuser' also covers 'testuser::data::*'
          #   unless a more-specific 'testuser::data' key overrides it.
          # - WU scope keys are the submitting user's login name (lowercase).
          - userName: testuser
            password: password
            defaults:
              resource: None
              fileScope: None
              eclWUScope: None
            resources:
              WsSMC:
                features:
                  SmcAccess: Full
                  ThorQueueAccess: Full
                  MachineInfoAccess: Full
                  ClusterTopologyAccess: Full
              WsWorkunits:
                features:
                  OwnWorkunitsAccess: Full
            fileScopes:
              # grants Full access to 'testuser::*' (and all descendant scopes)
              - name: "testuser"
                access: Full
              # example of a more-specific nested scope (overrides the parent if needed):
              # - name: "testuser::readonly"
              #   access: Read
            eclWUScopes:
              # grants Full access to workunits submitted by the user whose login is 'testuser'
              "testuser": Full

          # Password is valid but expired
          - userName: pwexpired
            password: password
            authenticateStatus: AS_PASSWORD_VALID_BUT_EXPIRED
            passwordExpiration: "2024-01-01"
            defaults:
              resource: Full
              fileScope: Full
              eclWUScope: Full
```

The `WsWorkunits` example is intentional: the platform auth maps define
`OwnWorkunitsAccess`, `OthersWorkunitsAccess`, and `DeployWorkunitsAccess` for the
`ws_workunits` service.

**File scope names** mirror the logical-file scope string: everything before the last `::` in
the logical filename (e.g. a file `testuser::data::myfile` has scope `testuser::data`).
Lookups are hierarchical — a `name` of `testuser` grants access to `testuser::*` at every
nesting depth unless a more-specific entry (e.g. `testuser::readonly`) overrides it.

**ECL WU scope keys** are simply the submitting user's login name (lowercase), which is what
the platform records as the workunit scope when a user submits a new workunit.

## Platform Environment Setup

### Bare Metal

1. Build the platform with `-DINCLUDE_PLUGINS=ON` (or specifically include
   `system/security/plugins/testauthSecurity` in your CMake configuration).
2. In the ESP component XML, add a `SecurityManagers/SecurityManager/<type>`
   section with the `userAccess` children as shown in the XML example above, and
   add an `<Authenticate method="testauthSecurity"/>` element to the relevant
   `EspBinding`.
3. Restart the ESP process (`hpcc-init restart esp`).

### Containerized

1. Add or merge the `authNZ.testauthSecurity` Helm values shown above into your deployment
   values file (e.g. `my-values.yaml`).
2. Deploy or upgrade:

   ```bash
   helm upgrade --install myhpcc hpcc/hpcc -f my-values.yaml
   ```

3. Verify that the ESP pod logs show `Test Auth Security Manager` initialising without
   errors.

> **Tip**: Use Kubernetes secrets (`@secretKey`) rather than plain-text `@password` even
> in test environments where the cluster may be shared.  Create the secret in the `authn`
> category and reference it by key name:
>
> ```yaml
> userAccess:
>   - userName: admin
>     secretKey: admin-testauth-secret
>     defaults:
>       resource: Full
> ```

## Configuration Reference

The following tables define the configuration model in a format-neutral way.
The same logical fields apply to both XML (`environment.xml`) and YAML (`values.yaml`).

### Canonical Model

#### `userAccess` Entry

| Field                | Required | Type   | Description                                                                                  |
|----------------------|----------|--------|----------------------------------------------------------------------------------------------|
| `userName`           | Yes      | string | Login username.                                                                              |
| `password`           | No       | string | Encrypted password. Defaults to `userName` when both `password` and `secretKey` are omitted. |
| `secretKey`          | No       | string | Kubernetes/Vault secret key whose `password` field is used instead of `password`.             |
| `authenticateStatus` | No       | string | `authStatus` enum name to apply on successful password verification (see table below).        |
| `passwordExpiration` | No       | date   | Password expiration date in `YYYY-MM-DD` format. Sets `ISecUser::setPasswordExpiration()`.   |

#### Child Structures

| Path                           | Description                                              |
|--------------------------------|----------------------------------------------------------|
| `defaults`                     | Default access levels (see below).                       |
| `resources.<service>.features` | Per-feature access flags for the named service type.     |
| `fileScopes`                   | File-scope access entries.                               |
| `eclWUScopes`                  | ECL workunit-scope access flags.                         |

#### `defaults` Fields

| Field         | Description                              |
|---------------|------------------------------------------|
| `resource`    | Default feature-resource access level.   |
| `fileScope`   | Default file-scope access level.         |
| `eclWUScope`  | Default ECL workunit-scope access level. |

#### `fileScopes` Entry

| Field    | Description                                 |
|----------|---------------------------------------------|
| `name`   | File-scope name (for example `a::b::c`).    |
| `access` | Access level for that file scope.           |

Valid access level strings: `Full`, `Write`, `Read`, `Access`, `None`, `Unavailable`.

### XML/YAML Field Mapping

| Canonical path                     | XML representation                                 | YAML representation                                |
|------------------------------------|----------------------------------------------------|----------------------------------------------------|
| `userAccess[].userName`            | `<userAccess userName="..."/>`                    | `userAccess: - userName: ...`                      |
| `userAccess[].password`            | `<userAccess password="..."/>`                    | `userAccess: - password: ...`                      |
| `userAccess[].secretKey`           | `<userAccess secretKey="..."/>`                   | `userAccess: - secretKey: ...`                     |
| `userAccess[].authenticateStatus`  | `<userAccess authenticateStatus="..."/>`          | `userAccess: - authenticateStatus: ...`            |
| `userAccess[].passwordExpiration`  | `<userAccess passwordExpiration="YYYY-MM-DD"/>`   | `userAccess: - passwordExpiration: "YYYY-MM-DD"` |
| `userAccess[].defaults.resource`   | `<defaults resource="..."/>`                      | `defaults: { resource: ... }`                      |
| `userAccess[].defaults.fileScope`  | `<defaults fileScope="..."/>`                     | `defaults: { fileScope: ... }`                     |
| `userAccess[].defaults.eclWUScope` | `<defaults eclWUScope="..."/>`                    | `defaults: { eclWUScope: ... }`                    |
| `userAccess[].fileScopes[].name`   | `<fileScope name="..."/>`                         | `fileScopes: - name: ...`                          |
| `userAccess[].fileScopes[].access` | `<fileScope access="..."/>`                       | `fileScopes: - access: ...`                        |
| `userAccess[].eclWUScopes[key]`    | `<eclWUScopes key="AccessLevel"/>`                | `eclWUScopes: { key: AccessLevel }`                |

### Value Resolution Rules

1. `secretKey` and `password` are optional. If neither is provided, the expected password defaults to `userName`.
2. If `secretKey` is provided, its `password` value is used as the expected password.
3. If `secretKey` is not provided and `password` is provided, `password` is used.
4. Access values are case-sensitive and must be one of: `Full`, `Write`, `Read`, `Access`, `None`, `Unavailable`.


### Authentication State Support

The plugin supports the full set of `authStatus` values defined in
`system/security/shared/seclib.hpp`.  When `authenticateStatus` is configured for a user
entry, authentication behaves as follows:

1. The supplied password is verified against the configured value.
2. If the password matches, the configured `authStatus` is set on the `ISecUser` object via
   `ISecUser::setAuthenticateStatus(...)`.
3. `authenticate()` returns `true` only when the configured status is `AS_AUTHENTICATED`
   (or when no status is configured at all—the default).  All other non-`AS_UNKNOWN` states
   cause `authenticate()` to return `false` with the status set, which drives the same ESP
   failure-handling paths that a real LDAP server would trigger.
4. If the password does **not** match, `authenticate()` returns `false` and no special
   status is applied.

| `authenticateStatus` value       | `authenticate()` returns | ESP behaviour                                     |
|----------------------------------|--------------------------|---------------------------------------------------|
| *(not set)*                      | `true`                   | Normal login                                      |
| `AS_AUTHENTICATED`               | `true`                   | Normal login                                      |
| `AS_PASSWORD_VALID_BUT_EXPIRED`  | `false`                  | Login denied - Non-API requests redirect to `/esp/updatepasswordinput`|
| `AS_PASSWORD_EXPIRED`            | `false`                  | Login denied – password expired                   |
| `AS_ACCOUNT_DISABLED`            | `false`                  | Login denied – "Account Disabled"                 |
| `AS_ACCOUNT_EXPIRED`             | `false`                  | Login denied – "Account Expired"                  |
| `AS_ACCOUNT_LOCKED`              | `false`                  | Login denied – "Account Locked"                   |
| `AS_ACCOUNT_ROOT_ACCESS_DENIED`  | `false`                  | Login denied – root access denied                 |
| `AS_INVALID_CREDENTIALS`         | `false`                  | Login denied – invalid credentials                |
| `AS_UNEXPECTED_ERROR`            | `false`                  | Login denied – unexpected error                   |


## Security and PII Considerations

* This plugin is intended for **non-production** use only.
* Configuration files containing `@password` in plain text must not be committed to
  source control or shared externally.
* No personally identifiable information (PII) is collected or stored by this plugin.
  All user data is static configuration supplied by the operator.
