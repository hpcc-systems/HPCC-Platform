# Overview

These directions walk through the steps to configure and setup, from scratch, an out-of-the-box HPCC platform deployment using LDAP authentication.

The goal is for developers and other technical platform users to have available an LDAP Directory Server (DS) server they can use for testing and troubleshooting authentication and authorization features. These directions don't cover the steps needed for securing a production-grade DS instance and the communication between it and the platform components. Any deployment with this setup should be ephemeral, publicly unreachable, and not have access to sensitive data.

First we will set up a Docker container with a minimal 389ds LDAP Directory Server and a phpLdapAdmin (PLA) client to administer it. The PLA can be used to inspect your DS and perform any administrative tasks not supported by the ECL Watch client.

Then we'll cover the configuration changes to deploy a minimal HPCC platform, both non-containerized (bare-metal) and under Kubernetes, that uses 389ds for security.

## Initial State

When the platform launches it ensures the LDAP DS is initialized with "OUs" (organizational units) needed. OUs for groups, users and esp authorization resources are created (among others). An HPCC Admin user is created and added to the administrator's group.

If you need any other security settings beyond that, such as non-administrative users and groups, you'll need to create them using ECL Watch. The administrator account itself has the privileges to make these kinds of changes. You may see that the admin doesn't by default have _all_ permissions, but it can add any it needs to the admin group.

This setup has been tested with ECL Watch authentication and authorization. It should also work for file and workunit scopes, but that hasn't been confirmed.

Your customized setup will be saved to a persistent volume, and you can separately maintain different versions as needed.

# Docker LDAP Directory Service and Admin Setup

We'll run our 389ds and PLA in a Docker container launched from platform source: `dockerfiles/examples/ldap`.

These instructions create a persistent volume for the DS inside your directory, but you can customize the docker-compose file to use another location. All of the server state is stored in the mounted volume, so if you want to be able to easily switch between different configurations you can easily do so by switching the mounted directory at launch.

1. Copy the `dockerfiles/examples/ldap` folder to a location outside of your repo, say `${HOME}/ldap`. Edit the file `${HOME}/ldap/docker-compose.yaml`, customizing the admin password and PVC mount point:
    - Change `DS_DM_PASSWORD: "<directory_manager_pw>"` placeholder with the real password you want to use. This is the password that you will use in PLA to administer the DS, and that the HPCC platform will use to interact with it using LDAP.
    - Under volumes, replace `${HOME}/389ds-data` with the location of your choice.
2. From `${HOME}/ldap`, run:

    ```bash
    docker compose up -d
    ```
3. Create the database backend on the DS. This should be done only once- the first time you run the container with a new directory mounted:

    ```bash
    docker exec -i -t 389ds /usr/sbin/dsconf localhost backend create --suffix dc=example,dc=com --be-name userRoot
    ```
4. You should see the response "The database was successfully created". You can verify by seeing files created in `${HOME}/ldap` or by running this command and confirming that `cd=example,dc=com` is listed:

    ```bash
    docker exec -i -t 389ds /usr/sbin/dsconf localhost backend suffix list
    ```
5. Verify your server is running by using phpLdapAdmin. Visit `hpttp://localhost:8080` and login with username: "cn=Directory Manager" and password as you configured: <directory_manager_pw>.

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
            - `computer = localhost`
            - `netAddress = .`
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
