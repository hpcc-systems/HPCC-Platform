# Overview

These directions walk through the steps to configure and setup, from scratch, an out-of-the-box HPCC platform deployment using LDAP authentication.

The goal is for developers and other technical platform users to have available an LDAP Directory Server (DS) server they can use for testing and troubleshooting authentication and authorization features. These directions don't cover the steps needed for securing a production-grade DS instance and the communication between it and the platform components. Any deployment with this setup should be ephemeral, publicly unreachable, and not have access to sensitive data.

First we will set up a Docker container with a minimal 389ds LDAP Directory Server and a phpLdapAdmin (PLA) client to administer it. The PLA can be used to inspect your DS and perform any administrative tasks not supported by the ECL Watch client.

Then we'll cover the configuration changes to deploy a minimal HPCC platform, both non-containerized (bare-metal) and under Kubernetes, that uses 389ds for security.

## Initial State

When the platform launches it ensures the LDAP DS is initialized with the base DN (distinguished name) and OUs (organizational units) needed. Among others, OUs for groups, users and ESP authorization resources are created.  If present in the config, an HPCC Admin user is created, and added to the administrator's group.

If you need any other security settings beyond that, such as non-administrative users and groups, you'll need to create them using ECL Watch. The administrator account itself has the privileges to make these kinds of changes. You may see that the admin doesn't by default have _all_ permissions, but it can add any it needs to the admin group.

This setup has been tested with ECL Watch authentication and authorization. It should also work for file and workunit scopes, but that hasn't been confirmed.

Your customized setup will be saved to a persistent volume, and you can separately maintain different versions as needed.

# Docker LDAP Directory Service and Admin Setup

We'll run our 389ds and PLA in a Docker container launched from platform source: `dockerfiles/examples/ldap`.

These instructions create a persistent volume for the DS inside your directory, but you can customize the docker-compose file to use another location. All of the server state is stored in the mounted volume, so if you want to be able to easily switch between different configurations you can easily do so by switching the mounted directory at launch.

> **Mounted data volume:** Throughout this document, `${HOME}/389ds` refers to the host directory
> mounted into the 389ds container as `/data`. This is where all server state (database, TLS
> certificates, configuration) is persisted between restarts. The default is `${HOME}/389ds` as
> set in `docker-compose.yaml`. If you change it there, substitute your chosen path everywhere
> `${HOME}/389ds` appears in the commands below.

1. Copy the `dockerfiles/examples/ldap` folder to a location outside of your repo, say `${HOME}/ldap`. Edit the file `${HOME}/ldap/docker-compose.yaml`, customizing the admin password and PVC mount point:
    - Change `DS_DM_PASSWORD: "<directory_manager_pw>"` placeholder with the real password you want to use. This is the password that you will use in PLA to administer the DS, and that the HPCC platform will use to interact with it using LDAP.
    - Under volumes, replace `${HOME}/389ds` with the location of your choice.
2. From `${HOME}/ldap`, run:

    ```bash
    docker compose up -d
    ```
3. Create the LDAP backend (suffix) on the Directory Server. This configures the server to manage the `dc=example,dc=com` naming context.  This step should be done only once, the first time you run the container with a new directory mounted:

    ```bash
    docker exec -i -t 389ds /usr/sbin/dsconf localhost backend create --suffix dc=example,dc=com --be-name userRoot
    ```

    **What this does:** The `dsconf backend create` command configures the LDAP server to manage a specific DN subtree (in this case `dc=example,dc=com`). This is a server-side configuration that tells 389 Directory Server "I will handle queries for this naming context," but it does **not** create any actual directory entries yet.

4. Verify the backend was created successfully. You should see the response "The database was successfully created". You can also verify by checking that files were created in your mounted volume (`${HOME}/389ds` or your configured path from `${HOME}/ldap/docker-compose.yaml`), or by running this command and confirming that `dc=example,dc=com` is listed:

    ```bash
    docker exec -i -t 389ds /usr/sbin/dsconf localhost backend suffix list
    ```

    **Note:** At this point, phpLDAPadmin will not yet be able to browse the directory tree because the base DN entry itself doesn't exist. Only the server configuration has been set up.

5. Initialize the LDAP directory structure by running HPCC Platform for the first time. Before using phpLDAPadmin to manage the directory, you must start the HPCC Platform at least once (see sections below for bare-metal or containerized setup). On first startup, the platform will: 
    - Detect that the base DN entry (`dc=example,dc=com`) is missing
    - Automatically create the base DN entry and all necessary organizational units (OUs) for users, groups, and resources
    - Initialize the HPCCAdministrators group if it is a relative path
    - Add an admin user if present in the config

   After the platform has initialized the directory structure, you can verify and manage your LDAP server using phpLDAPadmin at `http://localhost:8080`. Login with:
   - **username:** `cn=Directory Manager`
   - **password:** `<directory_manager_pw>` (as configured in docker-compose.yaml)

   You should now see the complete directory tree including `dc=example,dc=com` and all HPCC-created organizational units.

## Enable TLS on the Test Server (Optional)

By default the 389ds test instance runs on plain LDAP (port 389). If you need to test LDAPS
(port 636) with TLS certificate validation, follow these steps after completing the initial
setup above. This uses a self-signed CA and server certificate.

> **Note:** This is only needed if you are testing the `ldapTLSValidation` HPCC configuration
> feature. See [LDAPCertificateValidation.md](LDAPCertificateValidation.md) for details on that
> feature.

### Step 1 — Generate Certificates

Run these commands on the host (outside Docker). The server certificate must include a
**Subject Alternative Name (SAN)** matching the hostname HPCC uses to connect to 389ds.
HPCC's bundled OpenLDAP uses OpenSSL, which requires a SAN — a `CN`-only certificate will
be rejected with a hostname mismatch error even if the `CN` matches.

Choose a directory to store your certificates and substitute it for `<ldap-certs-dir>`
throughout this section (e.g., `~/hpcc/ldap-certs`).

```bash
# Create your certificate directory
mkdir -p <ldap-certs-dir> && cd <ldap-certs-dir>

# Generate the CA private key and self-signed CA certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=LDAP-CA/O=Example/C=US"

# Generate the server private key
openssl genrsa -out server.key 2048

# Generate a CSR for the server with a SAN covering localhost
openssl req -new -key server.key -out server.csr \
  -subj "/CN=localhost/O=Example/C=US" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# Sign the server certificate with the CA, preserving the SAN
openssl x509 -req -days 3650 -in server.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1") \
  -out server.crt

# Verify the SAN is present
openssl x509 -in server.crt -noout -text | grep -A2 "Subject Alternative"
```

> **Hostname match:** The SAN in the server certificate must match the value HPCC uses to
> connect to 389ds. This value comes from the `netAddress` of the `Hardware/Computer` entry
> that the `LDAPServerProcess` instance points to in `environment.xml` — **not** the
> `netAddress` on the `Instance` element itself.
>
> - `netAddress="127.0.0.1"` → SAN must include `IP:127.0.0.1` (as in the example above).
>   This is the recommended setting for local development. **Do not use `netAddress="localhost"`**
>   — OpenLDAP's DNS hostname matching has a known issue with the literal string `localhost` and
>   will report a hostname mismatch even when the cert includes `DNS:localhost` in its SAN. Using
>   `127.0.0.1` triggers the IP address matching path, which works correctly.
> - `netAddress="."` → expands to the host machine's primary IP at runtime (e.g. `10.0.0.98`).
>   The SAN must then cover that IP: `subjectAltName=IP:10.0.0.98,DNS:localhost`
>
> Do not change the existing shared `Hardware/Computer` entry's `netAddress` to accommodate LDAP
> — that entry is used by all platform components. Instead add a dedicated `Computer` entry for
> the LDAP instance (see the bare-metal configuration section below for details).

> **CA cert file location:** The `ldapCACertFile` path must be readable by the `hpcc` user.
> A file under your home directory may be inaccessible if the home directory has restricted
> permissions (`drwxr-x---`), even if the file itself is world-readable. Place the CA cert in
> a shared location:
> ```bash
> sudo mkdir -p /etc/HPCCSystems/certs
> sudo cp <ldap-certs-dir>/ca.crt /etc/HPCCSystems/certs/ldap-ca.crt
> sudo chown root:hpcc /etc/HPCCSystems/certs/ldap-ca.crt
> sudo chmod 640 /etc/HPCCSystems/certs/ldap-ca.crt
> ```
> Then set `ldapCACertFile="/etc/HPCCSystems/certs/ldap-ca.crt"` in `environment.xml`.

### Step 2 — Install the Certificate on 389ds

389ds does not read PEM files directly — it stores TLS material in an NSS certificate database.
The CA cert is imported with `certutil`, and the server cert + private key must be bundled into
a PKCS12 file first, then imported with `pk12util`.

> **Nickname:** Choose a nickname for your server certificate (e.g., `HPCC-389ds-cert`). The
> `-name` value in the `openssl pkcs12` command sets this nickname and **must be used exactly**
> in the `dsconf rsa set --nss-cert-name` command that follows. It is case-sensitive.

```bash
# Create the TLS subdirectory in the mounted data volume
mkdir -p ${HOME}/389ds/tls

# Copy certificates into the mounted data volume
cp <ldap-certs-dir>/server.crt <ldap-certs-dir>/server.key <ldap-certs-dir>/ca.crt ${HOME}/389ds/tls/

# Import the CA certificate into the NSS database
# The NSS database is password-protected — pwdfile.txt holds the PIN
docker exec 389ds certutil -A \
  -d /etc/dirsrv/slapd-localhost \
  -n "LDAP-CA" \
  -t "CT,," \
  -i /data/tls/ca.crt \
  -f /etc/dirsrv/slapd-localhost/pwdfile.txt

# Bundle the server certificate and private key into a PKCS12 file
# The -name value becomes the NSS nickname — it must match --nss-cert-name below
openssl pkcs12 -export \
  -in <ldap-certs-dir>/server.crt -inkey <ldap-certs-dir>/server.key \
  -out <ldap-certs-dir>/server.p12 \
  -passout pass:"" \
  -name "HPCC-389ds-cert"

# Copy the bundle into the mounted volume
cp <ldap-certs-dir>/server.p12 ${HOME}/389ds/tls/

# Import the server certificate and private key into the NSS database
docker exec 389ds pk12util \
  -i /data/tls/server.p12 \
  -d /etc/dirsrv/slapd-localhost \
  -W "" \
  -k /etc/dirsrv/slapd-localhost/pwdfile.txt

# Verify both certificates appear in the NSS database
docker exec 389ds certutil -L -d /etc/dirsrv/slapd-localhost
# Expected output (or similar):
# Certificate Nickname                                         Trust Attributes
#                                                              SSL,S/MIME,JAR/XPI
#
# LDAP-CA                                                      CT,,
# HPCC-389ds-cert                                              u,u,u

# Tell 389ds which certificate to present for TLS
# Must match the -name value used in the openssl pkcs12 command above
docker exec 389ds dsconf localhost security rsa set \
  --nss-cert-name "HPCC-389ds-cert"

# Enable TLS on the directory server
docker exec 389ds dsconf localhost security enable

# Restart the container for TLS to take effect
docker restart 389ds
```

The TLS configuration is persisted in the mounted volume (`${HOME}/389ds`). Subsequent
`docker compose up` runs will have TLS enabled without repeating these steps.

### Step 3 — Verify TLS is Working

```bash
openssl s_client -connect localhost:636 -CAfile <ldap-certs-dir>/ca.crt </dev/null 2>&1 | grep -E "subject|issuer|Verify return"
```

A successful result shows `Verify return code: 0 (ok)`, with `subject` matching your server
cert CN and `issuer` matching your CA.

> **Note:** `openssl s_client` without `</dev/null` will hang waiting for stdin. Always redirect
> stdin to avoid this.

### Step 4 — Configure HPCC to Validate the Certificate

Copy `ca.crt` to a stable path accessible by the `hpcc` user on each HPCC node:

```bash
sudo mkdir -p /etc/HPCCSystems/certs
sudo cp <ldap-certs-dir>/ca.crt /etc/HPCCSystems/certs/ldap-ca.crt
sudo chown root:hpcc /etc/HPCCSystems/certs/ldap-ca.crt
sudo chmod 640 /etc/HPCCSystems/certs/ldap-ca.crt
```

> **Note:** A file under your home directory may be inaccessible to the `hpcc` user if the home
> directory has restricted permissions (`drwxr-x---`), even if the file itself is world-readable.
> Use a shared location as shown above.

In `environment.xml`, add `ldapTLSValidation` and `ldapCACertFile` to the `<ldap>` element:

```xml
<ldap ldapTLSValidation="strict"
      ldapCACertFile="/etc/HPCCSystems/certs/ldap-ca.crt"
      ... />
```

For a Kubernetes deployment, deliver the CA cert via a `Secret` or `ConfigMap` and mount it into
the HPCC pods, then set the path in your Helm values:

```yaml
ldap:
  ldapTLSValidation: "strict"
  ldapCACertFile: "/etc/hpcc/certs/ldap-ca.crt"
```

Use `ldapTLSValidation="permissive"` first to diagnose any issues without hard-failing, then
switch to `"strict"` once connectivity is confirmed.

## Enable Password Expiration on the Test Server (Optional)

By default, 389ds does not enforce a password expiration policy — passwords never expire.
If you need to test HPCC's handling of expired 389ds passwords (tracked in
[GH#36810](https://github.com/hpcc-systems/HPCC-Platform/issues/36810)), you must explicitly
enable password policy on the test server.

> **Note:** This is only needed if you are testing password expiration behavior. It has no
> effect on any other part of the setup and can be skipped otherwise.

### Enable the Global Password Policy

Run these commands against your running 389ds container:

```bash
# Turn on password expiration and set the max age (in seconds).
# 600 seconds (10 minutes) is short enough to observe expiration without a long wait,
# while still leaving enough time to complete a manual test pass. Use a realistic value
# (e.g. 7776000 = 90 days) for anything other than active testing.
docker exec 389ds dsconf localhost pwpolicy set --pwdexpire on --pwdmaxage 600

# Optional: control how long before expiration the server starts warning on bind,
# and how many post-expiration "grace" binds are allowed before lockout.
docker exec 389ds dsconf localhost pwpolicy set --pwdwarning 300 --pwdgracelimit 3

# Verify the current policy
docker exec 389ds dsconf localhost pwpolicy get
```

> **Units:** 389ds password policy durations (`--pwdmaxage`, `--pwdwarning`) are in
> **seconds**, unlike Active Directory's `maxPwdAge` (100-nanosecond intervals). Don't reuse
> AD-style values here.

### Force an Existing Test User's Password to Expire Immediately

The `passwordExpirationTime` attribute is computed from the password's last-changed time plus
`--pwdmaxage`, so an already-set password won't reflect a newly-enabled or newly-changed
policy until it is changed again. To make an existing user's password expire immediately for
testing, either:

- Set a very small `--pwdmaxage` (as above) and wait for it to elapse, or
- Reset the user's password, which causes 389ds to recompute `passwordExpirationTime` using
  the current policy. You can do this via ECL Watch's "Reset Password" admin function, or
  directly against the test server:

  ```bash
  cat <<EOF | docker exec -i 389ds ldapmodify -x -D "cn=Directory Manager" -w <directory_manager_pw>
  dn: uid=<test-username>,ou=users,ou=ecl,dc=example,dc=com
  changetype: modify
  replace: userPassword
  userPassword: <new-password>
  EOF
  ```

### Verifying the Policy Took Effect

Use phpLDAPadmin (or `ldapsearch`) to inspect a test user's entry and confirm the
`passwordExpirationTime` operational attribute is now present:

```bash
docker exec 389ds ldapsearch -x -D "cn=Directory Manager" -w <directory_manager_pw> \
  -b "dc=example,dc=com" "(uid=<test-username>)" passwordExpirationTime
```

> **phpLDAPadmin note:** `passwordExpirationTime` is an operational attribute, so it is
> hidden on a user entry's default view. Open the entry and click **"Show internal
> attributes"** (near the bottom of the page) to reveal it.

A user whose password has not yet expired will show a `passwordExpirationTime` value in
generalized-time format (e.g. `20260101120000Z`, in UTC/GMT). If the attribute is absent,
double check that `--pwdexpire` is `on` and that the user's password was set (or reset) after
the policy was enabled.

### Scope of the Policy

The commands above set the **global** default policy (`cn=config`), which applies to all users
unless a local (per-subtree) policy overrides it. This is sufficient for most test scenarios.
See the [389ds password policy documentation](https://www.port389.org) for configuring local
policies scoped to a specific OU if your testing requires different policies for different
users.

> **Note:** `cn=Directory Manager` (the root DN / superuser) is exempt from password policy by
> design — it will never show a `passwordExpirationTime`, even with the policy enabled. Use a
> regular test user to validate expiration behavior.

### Disabling Password Expiration

To turn expiration testing back off without rebuilding the server, disable the global policy:

```bash
docker exec 389ds dsconf localhost pwpolicy set --pwdexpire off
```

> **Note:** Disabling the policy stops *new* expirations from being computed, but does **not**
> retroactively clear a `passwordExpirationTime` already stored on a test user — resetting the
> user's password with the policy off does *not* clear it either (verified: the old value is
> left untouched). To remove a stale `passwordExpirationTime` from a test user, delete the
> attribute directly:
>
> ```bash
> cat <<EOF | docker exec -i 389ds ldapmodify -x -D "cn=Directory Manager" -w <directory_manager_pw>
> dn: uid=<test-username>,ou=users,ou=ecl,dc=example,dc=com
> changetype: modify
> delete: passwordExpirationTime
> EOF
> ```

## Managing the Test Server Instance

Assuming stock values from our example, the state for the Directory Server is stored `${HOME}/389ds`. If you want to wipe out your server and start fresh, delete the contents of `${HOME}/389ds` and begin again with the setup above from step 2.

If you'd like to create an additional server with different settings, then:

1. Copy the `dockerfiles/examples/ldap` folder to a location outside of your repo to a new unique location, say `${HOME}/ldap-alternate`. Edit the file `${HOME}/ldap-alternate/docker-compose.yaml`, customizing the admin password and PVC mount point:
    - Change `DS_DM_PASSWORD: "<directory_manager_pw>"` placeholder with the real password you want to use. This is the password that you will use in PLA to administer the DS, and that the HPCC platform will use to interact with it using LDAP.
    - Under volumes, replace `${HOME}/389ds` with another new unique folder to hold the server state, say `${HOME}/389ds-alternate`.
2. From `${HOME}/ldap-alternate`, run:

    ```bash
    docker compose up -d
    ```
Then continue with steps 3-5 above, substituting your new `ldap-alternate` and `389ds-alternate` locations where appropriate. Now you have a second instance you can run instead of the first and its state is stored in the `389ds-alternate` folder.

# Bare-Metal Platform + Docker LDAP Setup

These directions assume you're starting with a vanilla configuration from a fresh install or build. If you're not, then adjust the component names to match your config.

## Configure the Platform

1. Create LDAP Server Process, keeping the default name "ldapserver"
    - Navigate to the Attributes tab and change these properties:
        - `adminGroupName = HPCCAdministrators`
        - `serverType = 389DirectoryServer`
        - `modulesBasedn = ou=SMC,ou=espservices,ou=ecl`
        - `systemBasedn = ou=example,ou=com`
        - `systemCommonName = Directory Manager`
        - `systemPassword = <directory_manager_pw>`
        - `systemUser = Directory Manager`
    - Navigate to the Instances tab:
        - Add an instance on:
            - `computer = ldap-local`
            - `netAddress = 127.0.0.1`

    > **TLS note:** If using LDAPS with certificate validation (`ldapTLSValidation`), the address
    > HPCC connects to is determined by the `netAddress` of the `Computer` entry in the
    > `Hardware` section of `environment.xml` that the LDAP instance's `computer` attribute points
    > to — **not** the `netAddress` on the `Instance` element itself. That `Hardware/Computer`
    > `netAddress` must match the CN or SAN in the server certificate.
    >
    > **Important:** Do not simply change the existing `Hardware/Computer` entry's `netAddress`
    > from `"."` to `"localhost"` — that entry is shared by all platform components and changing
    > it will break them (e.g. dfuserver). Instead, add a **separate** `Computer` entry in the
    > `Hardware` block dedicated to the LDAP instance:
    >
    > ```xml
    > <!-- In the <Hardware> block — add this alongside the existing Computer entry -->
    > <Computer computerType="linuxmachine"
    >           domain="localdomain"
    >           name="ldap-local"
    >           netAddress="127.0.0.1"/>
    > ```
    >
    > Then point the `LDAPServerProcess` instance at it:
    >
    > ```xml
    > <Instance computer="ldap-local" name="s1" netAddress="."/>
    > ```
    >
    > This writes `ldapAddress="127.0.0.1"` into the generated component config, matching the
    > `IP:127.0.0.1` SAN in the server certificate.
    > See [Step 1 of the TLS setup](#step-1--generate-certificates) for cert SAN requirements.
2. In the component "Esp - myesp", navigate to the Authentication tab and change these properties:
    - `ldapServer = ldapserver`
    - `method = ldap`
3. Hand-edit the active `environment.xml` file. Find the `<LDAPServerProcess>` element and add the attribute `hpccAdminSecretKey="myhpccadminsecretkey"`

### Notes

- `systemCommonName` and `systemUser` must match and they must match the name of the 'admin' account of the 389ds server. By default this is "Directory Manager".
- The `modulesBasedn` is changed here to match the location in the DS where the SmcAccess permission is located. On initialization the secmgr creates the HPCC Admin and gives it SmcAccess to be able to login to ECL Watch.
- `adminGroupName` value affects where the group is created in the DS hierarchy, so don't use the special values "Administrators" or "Directory Administrators" unless you know that's what you need. The suggested value "HPCCAdministrators" will work well for development and testing.
- `systemBasedn` is the root of the DS tree used by the platform. It must match the backend suffix used in step #3 when configuring the 389ds server.
- `systemPassword` must match the 389ds `DS_DM_PASSWORD` configured in the `dockerfiles/examples/ldap/docker-compose.yaml` file.

## Add Secret for HPCC Admin User

Next you must add the `myhpccadminsecretkey` to your HPCC platform deployment. If there are concerns about storing these credentials on disk you should remove this secret directory when not in use, or manage it with an encryption/decryption utility such as `gocryptfs`

First note the root of your platform install location, which we'll refer to as `<HPCC_ROOT>`. Package installs will be rooted in `/` but dev installs are typically located at `$HOME/runtime`.

1. Create all directories in this path if they don't already exist:

    ```
    <HPCC_ROOT>/opt/HPCCSystems/secrets/authn/myhpccadminsecretkey
    ```
2. Inside `myhpccadminsecretkey` create two text files with no extra whitespace and no terminal newline:
    - A file named `username` containing `hpcc_admin`
    - A file named `password` containing the password you want the hpcc admin user to have. We'll refer to it as `<hpcc_admin_pw>`

## Run and Use Platform

1. Start up the platform:

    ```bash
    hpcc-init start
    ```
2. Login to ECL Watch at [http://127.0.0.1/8010](http://127.0.0.1/8010) using the credentials:
    - user: `hpcc_admin`
    - password: `<hpcc_admin_pw>` (that you added to the secret above)

Initially your admin will only have SmcAccess, so you may see some access failure warnings, but you can navigate to the Topology | Security tab to customize the HPCCAdministrators permissions and add any other users, groups or permissions needed for testing.  These warnings could also be due to permissions caching, and may be resolved after a platform restart.

# Containerized Platform + Docker LDAP

This containerized deployment enables ldap auth for ECL Watch only, but it can be extended easily to other ESP services.

## Create k8s Secrets for Platform

You'll need two k8s secrets, one for the HPCC administrator and another for the LDAP server administrator. Once created they'll persist in the Kubernetes backing store.

1. Create secret for HPCC administrator:
    ```bash
    kubectl create secret generic myhpccadminsecretkey --from-literal=username=hpcc_admin --from-literal=password=<hpcc_admin_pw>
    ```
1. Create secret for the LDAP server administrator:
    ```bash
    kubectl create secret generic admincredssecretname --from-literal="username=Directory Manager" --from-literal=password=<directory_manager_pw>
    ```

## Customize Platform Helm Values

We'll be using the customized Helm values file at `helm/examples/ldap/hpcc-values.yaml`. It will run all standard ESP services, but it uses ldap authentication only for the eclwatch service. Customize this file further as needed.

### Notes

- The secrets values are case sensitive and must match what was created as k8s secrets. The supplied values work and match across instructions and files:
``` yaml
    admincredsmountname: "admincredssecretname"
    hpccadmincredsmountname: "myhpccadminsecretkey"
```
- `systemBasedn` is the root of the DS tree used by the platform. It must match the backend suffix used in step #3 when [configuring the 389ds server](#docker-ldap-directory-service-and-admin-setup).
- LDAP auth is only configured for the eclwatch service. Copy and paste the `auth` and `ldap` sections from there to any other ESP service you want to use ldap.

## Run and Use Platform

Run from the root of your platform source, or provide an absolute path to the Helm values file used below.

1. Start up the platform:

```bash
helm install mycluster hpcc/hpcc -f helm/examples/ldap/hpcc-values.yaml
```
2. Login to ECL Watch at [http://127.0.0.1/8010](http://127.0.0.1/8010) using the credentials:
    - user: `hpcc_admin`
    - password: `<hpcc_admin_pw>` Created in step [Add secret for HPCC Admin user](#add-secret-for-hpcc-admin-user)

Initially your admin will only have SmcAccess, so you may see some access failure warnings, but you can navigate to the Topology | Security tab to customize the HPCCAdministrators permissions and add any other users, groups or permissions needed for testing. These warnings could also be due to permissions caching, and may be resolved after a platform restart.
