## JWT Authorization Security Manager Plugin

The purpose of this plugin is to provide authentication and authorization capabilities for HPCC Systems users, with the
credentials passed via valid JWT tokens.

The intention is to adhere as closely as possibly to the OpenID Connect (OIC) specification, which is a simple identity
layer on top of the OAuth 2.0 protocol, while maintaining compatibility with the way HPCC Systems performs
authentication and authorization today.  More information about the OpenID Connect specification can be found at
<https://openid.net/specs/openid-connect-core-1_0.html>.

One of the big advantages of OAuth 2.0 and OIC is that the service (in this case, HPCC Systems) never interacts with the
user directly.  Instead, authentication is performed by a trusted third party and the (successful) results are passed to
the service in the form of a verifiable encoded token.

Unfortunately, HPCC Systems does not support the concept of third-party verification.  It assumes that users -- really,
any client application that operates as a user, including things like IDEs -- will submit username/password credentials
for authentication.  Until that is changed, HPCC Systems won't be able to fully adhere to the OIC specification.

We can, however, implement *most* of the specification.  That is what this plugin does.

NOTE: This plugin is not available in a Windows build.

### Code Documentation

Doxygen (<https://www.doxygen.nl/index.html>) can be used to create nice HTML documentation for the code.  Call/caller
graphs are also generated for functions if you have dot (https://www.graphviz.org/download/) installed and available on
your path.

Assuming ```doxygen``` is on your path, you can build the documentation via:

	cd system/security/plugins/jwtSecurity
	doxygen Doxyfile

The documentation can then be accessed via ```docs/html/index.html```.

### Theory of Operations

The plugin is called by the HPCC Systems ```esp``` process when a user needs to be authenticated.  That call will
contain the user's username and either a reference to a session token or a password.  The session token is present only
for already-authenticated users.

If the session token is not present, the plugin will call a ```JWT login service``` (also known as a JWT login endpoint)
with the username and password, plus a nonce value for additional security.

That service authenticates the username/password credentials.  If everything is good, the service constructs an
OIC-compatible token that includes authorization information for that user and returns it to the plugin.  The
token is validated according to the OIC specification, including signature verification.

Note that token signature verification requires an additional piece of information.  Tokens can be signed with a
hash-based algorithm or with a public key-based algorithm (the actual algorithm used is determined by the JWT service). 
To verify either kind of algorithm, the plugin will need either the secret hash key or the public key that matches what
the JWT service used.  That key is read by the plugin from a file, and the file is determined by a configuration setting
(see below).  It is possible to change the contents of that file without restarting the esp process.  Note, though, that
the plugin may not notice that the file's contents have changed for several seconds (changes do not **immediately** take
effect).

HPCC Systems uses a well-defined authorization scheme, originally designed around an LDAP implementation.  That scheme
is represented within the token as JWT claims.  This plugin will unpack those claims and map to the authorization checks
already in place within the HPCC Systems platform.

OIC includes the concept of refresh tokens.  Refresh tokens enable a service to re-authorize an existing token without
user intervention.  Re-authorization typically happens due to a token expiring.  Tokens should have a
relatively short lifetime -- e.g. 15-30 minutes -- to promote good security and also give administrators the ability to
modify a user's authorization while the user is logged in.  This plugin fully supports refresh tokens by validating
token lifetime at every authorization check and calling a ```JWT refresh service``` (also known as a JWT refresh
endpoint) when needed.  This largely follows the OIC specification.

### Deviations From OIC Specification

* Initial authentication:  As stated above, the ```esp``` process will gather the username/password credentials instead
of a third party, then send those credentials off to another service.  In a true OIC configuration, the client process
(the ```esp``` process) never sees user credentials and relies on an external service to gather them from the user.
* The request made to the ```JWT login service``` is a POST HTTP or HTTPS call (depending on your configuration)
containing four items in JSON format; example:

```
	{
		"username": "my_username",
		"password": "my_password",
		"client_id": "https://myhpcccluster.com",
		"nonce": "hf674DTRMd4Z1s"
	}
```
* The ```JWT login service``` should reply with an OIC-compatible JSON-formatted reply.  See
<https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse> for an example of a successful authentication and
<https://openid.net/specs/openid-connect-core-1_0.html#TokenErrorResponse> for an example of an error response.  The
token itself is comprised of several attributes, followed by HPCC Systems-specific claims; see
<https://openid.net/specs/openid-connect-core-1_0.html#IDToken> (all required fields are indeed required, plus the nonce
field).
	* Note that for success replies, the ```access_token``` and ```expires_in``` values are ignored by this plugin.
* If the token expires, the plugin will call the ```JWT refresh service``` to request a new token.  This follows
<https://openid.net/specs/openid-connect-core-1_0.html#RefreshTokens> except that the ```client_secret``` and ```scope```
fields in the request are omitted.

### Implications of Deviations

The most obvious outcome of this implementation is that a custom service/endpoint needs to be available.  Or rather two
services:  One to handle the initial user login and one to handle token refreshes.  Neither service *precisely* handles
requests and replies in an OIC-compatible way, but the tokens themselves *are* OIC-compatible, which is good.  That
allows you to use third-party JWT libraries to construct and validate those tokens.

### HPCC Systems Configuration Notes

Several items must be defined in the platform's configuration.  Within configmgr, the ```jwtsecmgr``` Security Manager
plugin must be added as a component and then modified according to your environment:

* The URL or unique name of this HPCC Systems cluster, used as the ```client_id``` in token requests
* Full URL to the ```JWT Login Endpoint``` (should be HTTPS, but not required)
* Full URL to the ```JWT Refresh Endpoint``` (should be HTTPS, but not required)
* Boolean indicating whether to accept self-signed certificates for those endpoints; defaults to false
* Secrets vault key/name or subdirectory under /opt/HPCCSystems/secrets/system in which the JWT key used for the chosen signature algorithm is stored; defaults to "jwt-security"
* Default permission access level (either "Full" or "None"); defaults to "Full"
* Default workunit scope access level (either "Full" or "None"); defaults to "Full"
* Default file scope access level (either "Full" or "None"); defaults to "Full"

Only the first three items have no default values and must be supplied.

Once the ```jwtseccmgr``` component is added, you have to tell other parts of the system to use the plugin.  For
user authentication and permissions affecting features and workunit scopes, you need to add the plugin to the ```esp```
component.  Instructions for doing so can be found in the HPCC Systems Administrator's Guide manual (though the
manual uses the htpasswd plugin as an example, the process is the same).

If you intend to implement file scope permissions then you will also need provide Dali information about the JWT
plugin.  In configmgr, within the ```Dali Server``` component, select the LDAP tab.  Change the ```authMethod``` entry
to ```secmgrPlugin``` and enter "jwtsecmgr" as the ```authPluginType```.  Make sure ```checkScopeScans``` is set to
true.

### HPCC Systems Authorization and JWT Claims

This plugin supports all authorizations documented in the HPCC Systems® Administrator's Guide with the exception of
"View Permissions".  Loosely speaking, the permissions are divided into three groups:  Feature, Workunit Scope, and File
Scope.

Feature permissions are supported exactly as documented.  A specific permission would exist as a JWT claim, by name,
with the associated value being the name of the permission.  For example, to grant read-only access to ECL Watch, use
this claim:

```
	{ "SmcAccess": "Read" }
```

File and workunit scope permissions are handled the same way, but different from feature permissions.  The claim is one
of the Claim constants in the tables below, and the associated value is a matching pattern.  A pattern can be simple
string or it can use wildcards (specifically, Linux's file globbing wildcards).  Wildcards are not typically needed.

Multiple patterns can be set for each claim.

#### Workunit Scope Permissions

Meaning|Claim|Value
-------|-----|-----
User has view rights to workunit scope|AllowWorkunitScopeView|*pattern*
User has modify rights to workunit scope|AllowWorkunitScopeModify|*pattern*
User has delete rights to workunit scope|AllowWorkunitScopeDelete|*pattern*
User does not have view rights to workunit scope|DenyWorkunitScopeView|*pattern*
User does not have modify rights to workunit scope|DenyWorkunitScopeModify|*pattern*
User does not have delete rights to workunit scope|DenyWorkunitScopeDelete|*pattern*

#### File Scope Permissions

Meaning|Claim|Value
-------|-----|-----
User has view rights to file scope|AllowFileScopeView|*pattern*
User has modify rights to file scope|AllowFileScopeModify|*pattern*
User has delete rights to file scope|AllowFileScopeDelete|*pattern*
User does not have view rights to file scope|DenyFileScopeView|*pattern*
User does not have modify rights to file scope|DenyFileScopeModify|*pattern*
User does not have delete rights to file scope|DenyFileScopeDelete|*pattern*