# LDAP Security Manager Init
This document covers the main steps taken by the LDAP Security Manager during initialization. It is important to
note that the LDAP Security Manager uses the LDAP protocol to access an Active Directory, AD. The AD is the store for
users, groups, permissions, resources, and more. The term LDAP is generally overused to refer to both.

## LDAP Instances ##
Each service and/or component using the LDAP security manager gets its own instance of the security manager. This 
includes a unique connection pool (see below). All operations described apply to each LDAP instance.

## Initialization Steps
The following sections cover the main steps taken during initialization

### Load Configuration
The following items are loaded from the configuration:

#### AD Hosts
The LDAP Security Manager supports using multiple ADs. The FQDN or IP address of each AD host is read from configuration
data and stored internally. The source is a comma separated list stored in the _ldapAddress_ config value. Each entry
is added to a pool of ADs.

**Note that all ADs are expected to use the same credentials and have the same configuration**

#### AD Credentials
AD credentials consist of a username and password. The LDAP security manager users these to perform all operations
on behalf of users and components in the cluster. There are three potential sources for credentials.

1. A Kubernetes secret. If the _ldapAdminSecretKey_ config value is set, but _ldapAdminVaultId_ is not (see 2) then
the AD credentials are retrieved as Kubernetes secrets.
2. If both _ldapAdminSecretKey_ and _ldapAdminVaultId_ config values are present, the AD credentials are retrieved
from the vault.
3. Hardcoded values from the _systemCommonName_ and _systemPassword_ config values stored in the environment.xml file.

As stated above, when multiple ADs are in use, the configuration of each must be the same. This includes credentials.

### Retrieve Server Information from the AD ###
During initialization, the security manager begins incrementing through the set of defined ADs until it is able to 
connect and retrieve information from an AD. Once retrieved, the information is used for all ADs (see statement 
above about all ADs being the same). The accessed AD is marked as the current AD and no other ADs are accessed
during initialization.

The retrieved information is used to verify the AD type so the security manager adjusts for variations between 
types. Additionally, defined DNs may be adjusted to match AD type requirements.

## Connections
The manager handles connections to an AD in order to perform required operations. It is possible that values such
as permissions and resources may be cached to improve performance.

### Connection Pool
The LDAP security manager maintains a pool of LDAP connections. The pool is limited in size to _maxConnections_ 
from the configuration. The connection pool starts empty. As connections are created, each is added to the pool
until the max allowed is reached. 

The following process is used when an LDAP connection is needed.

First, the connection pool is searched for a free connection. If found and valid, the connection is returned. A 
connection is considered free if no one is using it and valid if the AD can be accessed. If no valid free connections 
are found, a new uninitialized connection is created.

For a new connection, an attempt is made to connect to each AD starting with the current. See **Handling AD Hosts**
below for how ADs are cycled when a connection fails. For each AD, as it cycles through, connection attempts are 
retried with a short delay between each. If unable to connect, the AD host is marked rejected and the next is 
attempted. Once a new connection has been established, if the max number of connections has not been reached yet, 
the connection is added to the pool. 

It is important to note that if the pool has reached its max size, new connections will continue to be made, but
are not saved in the pool. This allows the pool to maintain a steady working state, but allow for higher demand. 
Connections not saved to the pool are deleted once no longer in use.

### Handling AD Hosts
The manager keeps a list of AD hosts and the index of the current host. The current host is used for all AD
operations until there is a failure. At that time the manager marks the host as "rejected" and moves to the 
next host using a round-robin scheme.