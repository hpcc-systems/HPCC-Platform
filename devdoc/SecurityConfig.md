# Security Configuration #
This document covers security configuration values and meanings. It does not serve as
the source for how to configure security, but rather what the different values mean. These
are not covered in the docs nor does any reasonable help information exist in the
config manager or yaml files.

## Supported Configurations ##
Security is configured either through an LDAP server or a plugin. Additionally, these are
supported in both legacy deployments that use _environment.xml_ and containerized deployments 
using Kubernetes and Helm charts. While these methods differ, the configuration values
remain the same. Focus is placed on the different values and not the deployment
target. Differences based on deployment can be found in the relevant platform documents.

## Security Managers ##
Security is implemented via a security manager interface. Managers are loaded and used by components within the
system to check authorization and authentication. LDAP is an exception to the loadable manager model. It is not a compliant
loadable module like other security plugins. For that reason, the configuration for each is separated into two 
sections below: LDAP and Plugin Security Managers.

### LDAP ###
LDAP is a protocol that connects to an Active Directory server (AD). The term LDAP is used
interchangeably with AD. Below are the configuration values for an LDAP connection. These are valid for both legacy
(environment.xml) and containerized deployments. For legacy deployments the configuration manager is the 
primary vehicle for setting these values. However, some values are not available through the tool and must be set 
manually in the environment.xml if needed for a legacy deployment.

In containerized environments, a LDAP configuration block is required for each component. Currently, this results in 
a verbose configuration where much of the information is repeated. 

LDAP is capable if handling user authentication and feature access authorization (such as filescopes).

| Value              | Example                                             | Meaning                                                                                        |
|--------------------|-----------------------------------------------------|------------------------------------------------------------------------------------------------|
| adminGroupName     | HPCCAdmins                                          | Group name containing admin users for the AD                                                   |
| cacheTimeout       | 60                                                  | Timeout in minutes to keep cached security data                                                |
| ldapCipherSuite    | N/A                                                 | Used when AD is not up to date with latest SSL libs. <br/> AD admin must provide               |
| ldapPort           | 389 (default)                                       | Insecure port                                                                                  |
| ldapSecurePort     | 636 (default)                                       | Secure port over TLS                                                                           |
| ldapProtocol       | ldap                                                | **ldap** for insecure (default), using ldapPort<br/> **ldaps** for secure using ldapSecurePort | 
| ldapTimeoutSec     | 60 (default 5 for debug, 60 otherwise)              | Connection timeout to an AD before rollint to next AD                                          |
| serverType         | ActiveDirectory                                     | Identifies the type of AD server. (2)                                                          |
| filesBasedn        | ou=files,ou=ecl_kr,DC=z0lpf,DC=onmicrosoft,DC=com   | DN where filescopes are stored                                                                 |
| groupsBasedn       | ou=groups,ou=ecl_kr,DC=z0lpf,DC=onmicrosoft,DC=com  | DN where groups are stored                                                                     |
| modulesBaseDn      | ou=modules,ou=ecl_kr,DC=z0lpf,DC=onmicrosoft,DC=com | DN where permissions for resource are stored (1)                                               |
| systemBasedn       | OU=AADDC Users,DC=z0lpf,DC=onmicrosoft,DC=com       | DN where the system user is stored                                                             |
| usersBasedn        | OU=AADDC Users,DC=z0lpf,DC=onmicrosoft,DC=com       | DN where users are stored (3)                                                                  |
| systemUser         | hpccAdmin                                           | Appears to only be used for IPlanet type ADs, but may still be required                        |
| systemCommonName   | hpccAdmin                                           | AD username of user to proxy all AD operations                                                 |
| systemPassword     | System user password                                | AD user password                                                                               |
| ldapAdminSecretKey | none                                                | Key for Kubernetes secrets (4) (5)                                                             |
| ldapAdminVaultId   | none                                                | Vault ID used to load system username and password (5)                                         |
| ldapDomain         | none                                                | Appears to be a comma separated version of the AD domain name components (5)                   |
| ldapAddress        | 192.168.10.42                                       | IP address to the AD                                                                           |
| commonBasedn       | DC=z0lpf,DC=onmicrosoft,DC=com                      | Overrides the domain retrieved from the AD for the system user (5)                             |
| templateName       | none                                                | Template used when adding resources (5)                                                        |
| authMethod         | none                                                | Not sure yet                                                                                   |


Notes:

1. _modulesBaseDn_ is the same as _resourcesBaseDn_ The code looks for first for _modulesBaseDn_ and if not found will
search for _resourcesBaseDn_
2. Allowed values for _serverType_ are **ActiveDirectory**, **AzureActiveDirectory**, **389DirectoryServer**, 
**OpenLDAP**, **Fedora389**
3. For AzureAD, users are managed from the AD dashboard, not via ECLWatch or through LDAP
4. If present, _ldapAdminVaultId_ is read and _systemCommonName_ and _systemPassword_ are read from the 
Kubernetes secrets store and not from the LDAP config values
5. Must be configured manually in the environment.xml in legacy environments

### Plugin Security Managers ###
Plugin security managers are separate shared objects loaded and initialized by the system. The manager interface is
passed to components in order to provide necessary security functions. Each plugin has its own configuration. HPCC 
components can be configured to use a plugin as needed. 

#### httpasswd Security Manager ####
See documentation for the settings and how to enable.

#### Single User Security Manager ####
To be added.

#### JWT Security Manager ####
To be added

