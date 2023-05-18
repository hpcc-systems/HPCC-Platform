# Securing Your ESDL Service
## Choose a Security Manager
The first step towards securing your service is to choose a security manager.

The selected manager defines the pool of available users and related user data. The HPCC platform includes a number of security managers, and your deployment may include additional managers accessed via separately installed plugins.

## Identify the Required Security Data
After choosing a security manager, it is necessary to tell the ESP which of the manager's user data will be used by the service.

The ESP uses resources to map developer-defined names to manager-defined values. A developer-assigned name is mapped to a manager-defined key, and the manager uses the key to locate the value. These resources are combined into resource maps based on the type of resource, either a location, a feature, or a setting.

*A resource's developer-assigned name may match the manager-defined key, but this is neither required nor assured. No two managers are guaranteed to identify the same value using the same name. A manager may require additional context to look up a value than can be conveyed by a name alone. For these reasons, it is normal for a unique definition to be provided for each service and security manager pairing.*

### Location
Configured in Authenticate/Location, a location resource identifies a permission required to access specific service URL paths. In the legacy `configmgr` UI, the resource list is identified as `URL Authentication`.

No such resources are created by default. At least one such resource must be configured and successfully authorized by the security manager for any additional security to be applied. The default developer-defined name is `/`, applying to all paths.

### Feature
Configured in Authenticate/Feature, a feature resource identifies a permission required to process a service method.

No such resources are created by default, and none are required. If no resources are configured, location authorization is the only authorization controlling service access.

### Setting
Configured in Authenticate/Setting, a setting resource identifies a datum associated with a user. The value's meaning is unknown to the ESP, but may be used by ESDL integration scripts.

No such resources are created by default, and none are required.

## Apply Feature Requirements
Features authorization requirements may be defined in any combination of the ESDL definition, the ESDL binding, and ESDL integration scripts. Requirements included in the definition and binding are enforced before request processing begins. Requirements included in integration scripts are enforced during script processing.

### ESDL Definition
The `auth_feature` annotation may be used in the ESDL definition to specify requirements intended for all services bound to the definition. When added to the `ESPservice`, the indicated features are applied to all methods in the service. When added to an `ESPmethod`, the indicated features are applied to that method alone.

*Example 1: Basic Definition Requirements*

    ESPservice [auth_feature("MyServiceAccess:ACCESS")] MyService
    {
        ESPmethod [auth_feature("MyMethodAccess:READ")] MyMethod(MyMethodRequest, MyMethodResponse);
        ESPmethod [auth_feature("MyMethod2Access:WRITE")] MyMethod2(MyMethod2Request, MyMethod2Response);
        ESPmethod [auth_feature("MyMethod3Access:READ")] MyMethod3(MyMethod3Request, MyMethod3Response);
        ESPmethod [auth_feature("!*, MyMethod4Access:READ")] MyMethod4(MyMethod4Request, MyMethod4Response);
        ESPmethod MyMethod5(MyMethod5Request, MyMethod5Response);
        ESPmethod [auth_feature("NONE")] MyServiceStatus(MyServiceStatusRequest, MyServiceStatusResponse);
    };

| Method | Required Access |
| ------ | -------------- |
| MyMethod | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *READ* access to `MyMethodDefinitionAccess` is imposed by the method |
| MyMethod2 | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *WRITE* access to `MyMethod2DefinitionAccess` is imposed by the method. |
| MyMethod3 | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *READ* access is `MyMethod3DefinitionAccess` is imposed by the method. |
| MyMethod4 | *READ* access to `MyMethod4Access` is imposed by the method. |
| MyMethod5 | *ACCESS* access to `MyServiceAccess` is inherited from the service. |
| MyServiceStatus | No security is required. |

Example 1 illustrates that service-defined requirements are imposed on each method unless a method explicitly overrides them, and that methods may add additional requirements as needed.

*Example 2: Alternative Definition Requirements*

    ESPservice [auth_feature("{$service}Access:ACCESS, {$method}Access:READ")] MyService
    {
        ESPmethod MyMethod(MyMethodRequest, MyMethodResponse);
        ESPmethod [auth_feature("MyMethod2Access:WRITE")] MyMethod2(MyMethod2Request, MyMethod2Response);
        ESPmethod MyMethod3(MyMethod3Request, MyMethod3Response);
        ESPmethod [auth_feature("!{$service}Accesss")] MyMethod4(MyMethod4Request, MyMethod4Response);
        ESPmethod [auth_feature("!{$method}Access")] MyMethod5(MyMethod5Request, MyMethod5Response);
        ESPmethod [auth_feature("NONE")] MyServiceStatus(MyServiceStatusRequest, MyServiceStatusResponse);
    };

Example 2 demonstrates alternative markup to produce the same security requirements. By adding the method-specific requirement to the service annotation, methods with requirements that conform to a naming convention may omit the annotation altogether. Considering a scenario where each method requires a single method-specific resource, the service annotation can be used to define all security requirements for the service.

### ESDL Binding
What if the ESDL definition does not specify security and you cannot change it without breaking another service bound to the same definition?

What if the ESDL definition specifies security, but imposes requirements that you cannot satisfy?

The `auth_feature` attribute may be used in the ESDL binding to specify requirements intended for only that binding. When added to the `Binding/Definition` element, the indicated features are applied to all methods in the service. When added to an `Binding/Defintion/Methods/Method` element, the indicated features are applied to that method alone.

The attribute values generally behave in the same manner as the ESDL definition's annotation values. As the `ESPmethod` annotation can ignore or override requirements defined by its predecessor, the `ESPservice`, the `Binding/Definition` attribute can ignore or override requirements defined by its predecessors, starting with the `ESPmethod`, and the `Binding/Definition/Methods/Method` attribute can ignore or override requirements defined by its predecessors, starting with `Binding/Definition`.

*Example 3: Override of the ESDL definition*

    <Binding>
        <Definition auth_feature="NONE, {$service}Access:ACCESS, {$method}Access:READ">
            <Methods>
                <Method name="MyMethod"/>
                <Method name="MyMethod2" auth_feature="($method}Access:WRITE"/>
                <Method name="MyMethod3"/>
                <Method name="MyMethod4" auth_feature="!{$service}Access"/>
                <Method name="MyMethod5" auth_feature="!{$method}Access"/>
                <Method name="MyServiceStatus" auth_feature="NONE"/>
            </Methods>
        </Definition>
    </Binding>

| Method | Required Access |
| ------ | -------------- |
| MyMethod | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *READ* access to `MyMethodDefinitionAccess` is imposed by the method |
| MyMethod2 | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *WRITE* access to `MyMethod2DefinitionAccess` is imposed by the method. |
| MyMethod3 | *ACCESS* access to `MyServiceAccess` is inherited from the service, and *READ* access is `MyMethod3DefinitionAccess` is imposed by the method. |
| MyMethod4 | *READ* access to `MyMethod4Access` is imposed by the method. |
| MyMethod5 | *ACCESS* access to `MyServiceAccess` is inherited from the service. |
| MyServiceStatus | No security is required. |

Example 3 produces the same results as examples 1 and 2. The rationale for this example is that you either don't know or don't care what security is required by the definition you are using.

*Example 4: Location-only Authorization*

    <Binding>
        <Definition auth_feature="NONE">
            <Methods>
                <Method name="MyMethod"/>
                <Method name="MyMethod2"/>
                <Method name="MyMethod3"/>
                <Method name="MyMethod4"/>
                <Method name="MyMethod5"/>
                <Method name="MyServiceStatus"/>
            </Methods>
        </Definition>
    </Binding>

Example 4 shows how a binding reliant solely on location authorization can disable all security requirements imposed by the ESDL definition.

*Example 5: Simplifid Feature Security*

    <Binding>
        <Definition auth_feature="{$method}Access:Read">
            <Methods>
                <Method name="MyMethod"/>
                <Method name="MyMethod2" auth_feature="{$method}Access:WRITE"/>
                <Method name="MyMethod3"/>
                <Method name="MyMethod4"/>
                <Method name="MyMethod5" auth_feature="!{$method}Access"/>
                <Method name="MyServiceStatus" auth_feature="NONE"/>
            </Methods>
        </Definition>
    </Binding>

| Method | Required Access |
| ------ | -------------- |
| MyMethod | *READ* access to `MyMethodDefinitionAccess` is imposed by the service |
| MyMethod2 | *WRITE* access to `MyMethod2DefinitionAccess` is imposed by the method. |
| MyMethod3 | *READ* access is `MyMethod3DefinitionAccess` is imposed by the service. |
| MyMethod4 | *READ* access to `MyMethod4Access` is imposed by the service. |
| MyMethod5 | It is not recommended to set security requirements like this- an affirmative declaration is required. If there is an ESP Process configuration setting for `EspService/@defaultFeatureAuth` then that value is used, with a default value being *${service}Access:FULL*. If `@defaultFeatureAuth` is set to *no_default* then this security setting is invalid |
| MyServiceStatus | No security is required. |

Example 5 applies a single method-specific requirement on each method that does not explicitly override it.

### auth_feature Syntax

The evaluation of required feature security for a native service considers auth_feature values specified in an ESDL definition. The evaluation of required feature security for an ESDL service considers the same ESDL definition values as well as values from the ESDL binding. The ESDL binding values and a possible ESDL service default for all bindings are the only differences between the two scenarios.

Each `auth_feature` value is explicitly assigned a *scope* identifier and an order of precedence.

| Order of Precedence | Source | Scope Name |
| - | - | - |
| 1 (low) | ESPservice | EsdlService |
| 2 | ESPmethod | EsdlMethod |
| 3 | Binding/Definition | BindingService |
| 4 (high) | Binding/Definition/Methods/Method | BindingMethod |

In the event of conflicting requirements between scopes, the requirement from the scope with higher precedence takes effect. In the event of conflicting requirements within a scope, the right-most requirement takes effect.

    auth-feature-value ::= token [ ',' auth-feature-value ]
    token ::= ( exclusion | suppression | deferral | assignment )
    exclusion ::= ( exclude-all | exclude-scope | exclude-feature | exclude-feature-in-scope )
        ; no form of exclusion satisfies a requirement to affirmatively specify security
    exclude-all ::= '!*' [ '::*' ]
        ; all lower precedence tokens are ignored
        ; any lower precedence affirmation of security is ignored
    exclude-scope ::= '!' scope-name [ '::*' ]
        ; all lower precedence tokens specified by the named scope are ignored
        ; any lower precedence affirmation of security resulting from the named scope is ignored
    exclude-feature ::= '!' [ '*' ] '::' feature-name
        ; any lower precedence token specifying the named feature is ignored
        ; any lower precedence affirmation of security resulting from the named feature is ignored
    exclude-feature-in-scope ::= '!' scope-name '::' feature-name
        ; any lower precedence token specifying the named feature and specified in the named scope is ignored
        ; any lower precedence affirmation of security resulting from the named scope and feature is ignored
    suppression ::= ( suppress-all | suppress-feature )
    suppress-all ::= 'NONE'
        ; all lower precedence tokens are ignored
        ; the absence of security is affirmed
    suppress-feature ::= feature-name ':NONE'
        ; any lower precedence token specifying the named feature is ignored
        ; any lower precedence affirmation of security resulting from the named feature is ignored
    deferral ::= ( defer-all | defer-feature )
    defer-all ::= 'DEFERRED'
        ; the current security state, which may or may not be empty, is affirmed
    defer-feature ::= feature-name ':DEFERRED'
        ; any lower precedence token specifying the named feature is ignored
        ; a new map entry is created that requires no security
        ; the security state is affirmed
    assignment ::= ( assign-default-level | assign-default-feature | assign-feature-and-level )
    assign-default-level ::= feature-name
        ; any lower precedence token specifying the named feature is ignored
        ; a new map entry is created that requires full access-level
        ; the security state is affirmed
    assign-default-feature ::= ':' access-level
        ; any lower precedence token specifying the feature-name equivalent to '${service}Access' is ignored
        ; a new map entry is created specifying the feature-name equivalent to '${service}Access' and the given access level
        ; the security state is affirmed
    assign-feature-and-level ::= feature-name ':' access-level
        ; any lower priority token specifying the named feature is made obsolete
        ; a new map entry is created specifying the named feature and the given access level
        ; the security state is affirmed
    scope-name ::= ( 'DEFAULT' | 'ESDLSERVICE' | 'ESDLMETHOD' | ... )
        ; additional names are anticipated resulting from binding integration
    access-level ::= ( 'ACCESS' | 'READ' | 'WRITE' | 'FULL' )
    reserved-word ::= scope-name | access-level
    feature-name ::= ( feature-name-char* variable-ref-start variable-name '}' feature-name-char* | feature-name-char+ )
        ; feature-name-char is any char except whitespace, ',', '"', '!', or ':'
        ; feature-name has the added restrictions of not being a reserved-word
    variable-name ::= ( 'SERVICE' | 'METHOD' )
    variable-ref-start ::= ( '${' | '{$' )