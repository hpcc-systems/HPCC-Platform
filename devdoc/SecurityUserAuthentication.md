# User Authentication
This document covers user authentication, the process of verifying the identity of the user. 
Authorization is a separate topic covering whether a user should be allowed to perform a 
specific operation of access a specific resource.

Each supported security manager is covered.

Generally, when authentication is needed, the security manager client should call the _ISecManager_ 
_authenticateUser_ method. The method also allows the caller to detect if the user being authenticated
is a superuser. Use of that feature is beyond the scope of this document. In practice, this method is rarely
if ever called. User authentication is generally performed as part of authorization. This is covered in
more detail below.

## Security Manager User Authentication
This section covers how each supported security manager handles user authentication. As stated above, the
method _authenticateUser_ is defined for this purpose. However, other methods also perform user authentication.
The sections that follow describe in general how each security manager performs user authentication, whether
from directly calling the _authenticateUser_ method, or as an ancillary action taken when another method
is called.

### LDAP 
The LDAP security manager uses the configured Active Directory to authenticate users. Once authenticated,
the user is added to the permissions cache, if enabled, to prevent repeated trips to the AD whenever an
authentication check is required. 

If caching is enabled, a lookup is done to see if the user is already cached. If so, the cached user authentication
status is returned. Note that the cached status remains until either the cache time to live expires or 
is cleared either manually or through some other programmatic action.

If caching is not enabled, a request is sent to the AD to validate the user credentials.

In either case, if digital signatures are configured, the user is also digitally signed using the username. Digitally
signing the user allows for quick authentication by validating the signature against the username. During initial
authentication, if the digital signature exists, it is verified to provide a fast way to authenticate the user. If the
signature is not verified, the user is marked as not authenticated.

Authentication status is stored in the security user object so that further checks are not necessary when the same 
user object is used in multiple calls to the security manager.

### HTPasswd
Authentication in the htpasswd manager does not support singularly authenticating the user without also 
authorizing resource access. See the special case for authentication with authorization below.

Regardless, the htpasswd manager authenticates users using the _.htpasswd_ file that is installed on the
cluster. It does so by finding the user in the file and verifying that the input hashed password matches
the stored hashed password in file.

### Single User
The single user security manager allows the definition of a single username with a password. The values are
set in the environment configuration and are read during the initialization of the manager. All authentication 
requests validate against the configured username and password. The process is a simple comparison. Note
that the password stored in the environment is hashed.


## User Authentication During Authorization
Since resource access authorization requires an authenticated user, the authorization process also authenticates
the user before checking authorization. There are a couple of advantages to this 
* Two separate calls are not required to check authorization (one to verify the user and one to 
check authorization)
* The caller can perform third party authorization for specific user access

The _authenticate_ method, or any of its overloads or derivatives, accepts a resource or resource list and a user. 
These methods authenticate the user first before checking access to the specified resource.

ECL Watch uses user authentication during authorization during its log in process. Instead of first authenticating 
the user, it calls an authenticate method passing both the user and the necessary resources for which the user must 
have access in order to log into ECL Watch.
