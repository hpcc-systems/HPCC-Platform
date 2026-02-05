import{_ as i,y as o,a,o as n,ag as r,D as d}from"./chunks/framework.Do1Zayaf.js";const y=JSON.parse('{"title":"Securing Your ESDL Service","description":"","frontmatter":{},"headers":[],"relativePath":"esp/esdllib/README-SECURITY.md","filePath":"esp/esdllib/README-SECURITY.md","lastUpdated":1770295448000}'),s={name:"esp/esdllib/README-SECURITY.md"};function c(u,e,h,l,m,f){const t=o("RenderComponent");return n(),a("div",null,[e[0]||(e[0]=r(`<h1 id="securing-your-esdl-service" tabindex="-1">Securing Your ESDL Service <a class="header-anchor" href="#securing-your-esdl-service" aria-label="Permalink to &quot;Securing Your ESDL Service&quot;">​</a></h1><h2 id="choose-a-security-manager" tabindex="-1">Choose a Security Manager <a class="header-anchor" href="#choose-a-security-manager" aria-label="Permalink to &quot;Choose a Security Manager&quot;">​</a></h2><p>The first step towards securing your service is to choose a security manager.</p><p>The selected manager defines the pool of available users and related user data. The HPCC platform includes a number of security managers, and your deployment may include additional managers accessed via separately installed plugins.</p><h2 id="identify-the-required-security-data" tabindex="-1">Identify the Required Security Data <a class="header-anchor" href="#identify-the-required-security-data" aria-label="Permalink to &quot;Identify the Required Security Data&quot;">​</a></h2><p>After choosing a security manager, it is necessary to tell the ESP which of the manager&#39;s user data will be used by the service.</p><p>The ESP uses resources to map developer-defined names to manager-defined values. A developer-assigned name is mapped to a manager-defined key, and the manager uses the key to locate the value. These resources are combined into resource maps based on the type of resource, either a location, a feature, or a setting.</p><p><em>A resource&#39;s developer-assigned name may match the manager-defined key, but this is neither required nor assured. No two managers are guaranteed to identify the same value using the same name. A manager may require additional context to look up a value than can be conveyed by a name alone. For these reasons, it is normal for a unique definition to be provided for each service and security manager pairing.</em></p><h3 id="location" tabindex="-1">Location <a class="header-anchor" href="#location" aria-label="Permalink to &quot;Location&quot;">​</a></h3><p>Configured in Authenticate/Location, a location resource identifies a permission required to access specific service URL paths. In the legacy <code>configmgr</code> UI, the resource list is identified as <code>URL Authentication</code>.</p><p>No such resources are created by default. At least one such resource must be configured and successfully authorized by the security manager for any additional security to be applied. The default developer-defined name is <code>/</code>, applying to all paths.</p><h3 id="feature" tabindex="-1">Feature <a class="header-anchor" href="#feature" aria-label="Permalink to &quot;Feature&quot;">​</a></h3><p>Configured in Authenticate/Feature, a feature resource identifies a permission required to process a service method.</p><p>No such resources are created by default, and none are required. If no resources are configured, location authorization is the only authorization controlling service access.</p><h3 id="setting" tabindex="-1">Setting <a class="header-anchor" href="#setting" aria-label="Permalink to &quot;Setting&quot;">​</a></h3><p>Configured in Authenticate/Setting, a setting resource identifies a datum associated with a user. The value&#39;s meaning is unknown to the ESP, but may be used by ESDL integration scripts.</p><p>No such resources are created by default, and none are required.</p><h2 id="apply-feature-requirements" tabindex="-1">Apply Feature Requirements <a class="header-anchor" href="#apply-feature-requirements" aria-label="Permalink to &quot;Apply Feature Requirements&quot;">​</a></h2><p>Features authorization requirements may be defined in any combination of the ESDL definition, the ESDL binding, and ESDL integration scripts. Requirements included in the definition and binding are enforced before request processing begins. Requirements included in integration scripts are enforced during script processing.</p><h3 id="esdl-definition" tabindex="-1">ESDL Definition <a class="header-anchor" href="#esdl-definition" aria-label="Permalink to &quot;ESDL Definition&quot;">​</a></h3><p>The <code>auth_feature</code> annotation may be used in the ESDL definition to specify requirements intended for all services bound to the definition. When added to the <code>ESPservice</code>, the indicated features are applied to all methods in the service. When added to an <code>ESPmethod</code>, the indicated features are applied to that method alone.</p><p><em>Example 1: Basic Definition Requirements</em></p><pre><code>ESPservice [auth_feature(&quot;MyServiceAccess:ACCESS&quot;)] MyService
{
    ESPmethod [auth_feature(&quot;MyMethodAccess:READ&quot;)] MyMethod(MyMethodRequest, MyMethodResponse);
    ESPmethod [auth_feature(&quot;MyMethod2Access:WRITE&quot;)] MyMethod2(MyMethod2Request, MyMethod2Response);
    ESPmethod [auth_feature(&quot;MyMethod3Access:READ&quot;)] MyMethod3(MyMethod3Request, MyMethod3Response);
    ESPmethod [auth_feature(&quot;!*, MyMethod4Access:READ&quot;)] MyMethod4(MyMethod4Request, MyMethod4Response);
    ESPmethod MyMethod5(MyMethod5Request, MyMethod5Response);
    ESPmethod [auth_feature(&quot;NONE&quot;)] MyServiceStatus(MyServiceStatusRequest, MyServiceStatusResponse);
};
</code></pre><table tabindex="0"><thead><tr><th>Method</th><th>Required Access</th></tr></thead><tbody><tr><td>MyMethod</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>READ</em> access to <code>MyMethodDefinitionAccess</code> is imposed by the method</td></tr><tr><td>MyMethod2</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>WRITE</em> access to <code>MyMethod2DefinitionAccess</code> is imposed by the method.</td></tr><tr><td>MyMethod3</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>READ</em> access is <code>MyMethod3DefinitionAccess</code> is imposed by the method.</td></tr><tr><td>MyMethod4</td><td><em>READ</em> access to <code>MyMethod4Access</code> is imposed by the method.</td></tr><tr><td>MyMethod5</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service.</td></tr><tr><td>MyServiceStatus</td><td>No security is required.</td></tr></tbody></table><p>Example 1 illustrates that service-defined requirements are imposed on each method unless a method explicitly overrides them, and that methods may add additional requirements as needed.</p><p><em>Example 2: Alternative Definition Requirements</em></p><pre><code>ESPservice [auth_feature(&quot;{$service}Access:ACCESS, {$method}Access:READ&quot;)] MyService
{
    ESPmethod MyMethod(MyMethodRequest, MyMethodResponse);
    ESPmethod [auth_feature(&quot;MyMethod2Access:WRITE&quot;)] MyMethod2(MyMethod2Request, MyMethod2Response);
    ESPmethod MyMethod3(MyMethod3Request, MyMethod3Response);
    ESPmethod [auth_feature(&quot;!{$service}Accesss&quot;)] MyMethod4(MyMethod4Request, MyMethod4Response);
    ESPmethod [auth_feature(&quot;!{$method}Access&quot;)] MyMethod5(MyMethod5Request, MyMethod5Response);
    ESPmethod [auth_feature(&quot;NONE&quot;)] MyServiceStatus(MyServiceStatusRequest, MyServiceStatusResponse);
};
</code></pre><p>Example 2 demonstrates alternative markup to produce the same security requirements. By adding the method-specific requirement to the service annotation, methods with requirements that conform to a naming convention may omit the annotation altogether. Considering a scenario where each method requires a single method-specific resource, the service annotation can be used to define all security requirements for the service.</p><h3 id="esdl-binding" tabindex="-1">ESDL Binding <a class="header-anchor" href="#esdl-binding" aria-label="Permalink to &quot;ESDL Binding&quot;">​</a></h3><p>What if the ESDL definition does not specify security and you cannot change it without breaking another service bound to the same definition?</p><p>What if the ESDL definition specifies security, but imposes requirements that you cannot satisfy?</p><p>The <code>auth_feature</code> attribute may be used in the ESDL binding to specify requirements intended for only that binding. When added to the <code>Binding/Definition</code> element, the indicated features are applied to all methods in the service. When added to an <code>Binding/Defintion/Methods/Method</code> element, the indicated features are applied to that method alone.</p><p>The attribute values generally behave in the same manner as the ESDL definition&#39;s annotation values. As the <code>ESPmethod</code> annotation can ignore or override requirements defined by its predecessor, the <code>ESPservice</code>, the <code>Binding/Definition</code> attribute can ignore or override requirements defined by its predecessors, starting with the <code>ESPmethod</code>, and the <code>Binding/Definition/Methods/Method</code> attribute can ignore or override requirements defined by its predecessors, starting with <code>Binding/Definition</code>.</p><p><em>Example 3: Override of the ESDL definition</em></p><pre><code>&lt;Binding&gt;
    &lt;Definition auth_feature=&quot;NONE, {$service}Access:ACCESS, {$method}Access:READ&quot;&gt;
        &lt;Methods&gt;
            &lt;Method name=&quot;MyMethod&quot;/&gt;
            &lt;Method name=&quot;MyMethod2&quot; auth_feature=&quot;($method}Access:WRITE&quot;/&gt;
            &lt;Method name=&quot;MyMethod3&quot;/&gt;
            &lt;Method name=&quot;MyMethod4&quot; auth_feature=&quot;!{$service}Access&quot;/&gt;
            &lt;Method name=&quot;MyMethod5&quot; auth_feature=&quot;!{$method}Access&quot;/&gt;
            &lt;Method name=&quot;MyServiceStatus&quot; auth_feature=&quot;NONE&quot;/&gt;
        &lt;/Methods&gt;
    &lt;/Definition&gt;
&lt;/Binding&gt;
</code></pre><table tabindex="0"><thead><tr><th>Method</th><th>Required Access</th></tr></thead><tbody><tr><td>MyMethod</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>READ</em> access to <code>MyMethodDefinitionAccess</code> is imposed by the method</td></tr><tr><td>MyMethod2</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>WRITE</em> access to <code>MyMethod2DefinitionAccess</code> is imposed by the method.</td></tr><tr><td>MyMethod3</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service, and <em>READ</em> access is <code>MyMethod3DefinitionAccess</code> is imposed by the method.</td></tr><tr><td>MyMethod4</td><td><em>READ</em> access to <code>MyMethod4Access</code> is imposed by the method.</td></tr><tr><td>MyMethod5</td><td><em>ACCESS</em> access to <code>MyServiceAccess</code> is inherited from the service.</td></tr><tr><td>MyServiceStatus</td><td>No security is required.</td></tr></tbody></table><p>Example 3 produces the same results as examples 1 and 2. The rationale for this example is that you either don&#39;t know or don&#39;t care what security is required by the definition you are using.</p><p><em>Example 4: Location-only Authorization</em></p><pre><code>&lt;Binding&gt;
    &lt;Definition auth_feature=&quot;NONE&quot;&gt;
        &lt;Methods&gt;
            &lt;Method name=&quot;MyMethod&quot;/&gt;
            &lt;Method name=&quot;MyMethod2&quot;/&gt;
            &lt;Method name=&quot;MyMethod3&quot;/&gt;
            &lt;Method name=&quot;MyMethod4&quot;/&gt;
            &lt;Method name=&quot;MyMethod5&quot;/&gt;
            &lt;Method name=&quot;MyServiceStatus&quot;/&gt;
        &lt;/Methods&gt;
    &lt;/Definition&gt;
&lt;/Binding&gt;
</code></pre><p>Example 4 shows how a binding reliant solely on location authorization can disable all security requirements imposed by the ESDL definition.</p><p><em>Example 5: Simplifid Feature Security</em></p><pre><code>&lt;Binding&gt;
    &lt;Definition auth_feature=&quot;{$method}Access:Read&quot;&gt;
        &lt;Methods&gt;
            &lt;Method name=&quot;MyMethod&quot;/&gt;
            &lt;Method name=&quot;MyMethod2&quot; auth_feature=&quot;{$method}Access:WRITE&quot;/&gt;
            &lt;Method name=&quot;MyMethod3&quot;/&gt;
            &lt;Method name=&quot;MyMethod4&quot;/&gt;
            &lt;Method name=&quot;MyMethod5&quot; auth_feature=&quot;!{$method}Access&quot;/&gt;
            &lt;Method name=&quot;MyServiceStatus&quot; auth_feature=&quot;NONE&quot;/&gt;
        &lt;/Methods&gt;
    &lt;/Definition&gt;
&lt;/Binding&gt;
</code></pre><table tabindex="0"><thead><tr><th>Method</th><th>Required Access</th></tr></thead><tbody><tr><td>MyMethod</td><td><em>READ</em> access to <code>MyMethodDefinitionAccess</code> is imposed by the service</td></tr><tr><td>MyMethod2</td><td><em>WRITE</em> access to <code>MyMethod2DefinitionAccess</code> is imposed by the method.</td></tr><tr><td>MyMethod3</td><td><em>READ</em> access is <code>MyMethod3DefinitionAccess</code> is imposed by the service.</td></tr><tr><td>MyMethod4</td><td><em>READ</em> access to <code>MyMethod4Access</code> is imposed by the service.</td></tr><tr><td>MyMethod5</td><td>It is not recommended to set security requirements like this- an affirmative declaration is required. If there is an ESP Process configuration setting for <code>EspService/@defaultFeatureAuth</code> then that value is used, with a default value being <em><span id="fence-1-1"> ... </span>Access:FULL</em>. If <code>@defaultFeatureAuth</code> is set to <em>no_default</em> then this security setting is invalid</td></tr><tr><td>MyServiceStatus</td><td>No security is required.</td></tr></tbody></table><p>Example 5 applies a single method-specific requirement on each method that does not explicitly override it.</p><h3 id="auth-feature-syntax" tabindex="-1">auth_feature Syntax <a class="header-anchor" href="#auth-feature-syntax" aria-label="Permalink to &quot;auth_feature Syntax&quot;">​</a></h3><p>The evaluation of required feature security for a native service considers auth_feature values specified in an ESDL definition. The evaluation of required feature security for an ESDL service considers the same ESDL definition values as well as values from the ESDL binding. The ESDL binding values and a possible ESDL service default for all bindings are the only differences between the two scenarios.</p><p>Each <code>auth_feature</code> value is explicitly assigned a <em>scope</em> identifier and an order of precedence.</p><table tabindex="0"><thead><tr><th>Order of Precedence</th><th>Source</th><th>Scope Name</th></tr></thead><tbody><tr><td>1 (low)</td><td>ESPservice</td><td>EsdlService</td></tr><tr><td>2</td><td>ESPmethod</td><td>EsdlMethod</td></tr><tr><td>3</td><td>Binding/Definition</td><td>BindingService</td></tr><tr><td>4 (high)</td><td>Binding/Definition/Methods/Method</td><td>BindingMethod</td></tr></tbody></table><p>In the event of conflicting requirements between scopes, the requirement from the scope with higher precedence takes effect. In the event of conflicting requirements within a scope, the right-most requirement takes effect.</p><pre><code>auth-feature-value ::= token [ &#39;,&#39; auth-feature-value ]
token ::= ( exclusion | suppression | deferral | assignment )
exclusion ::= ( exclude-all | exclude-scope | exclude-feature | exclude-feature-in-scope )
    ; no form of exclusion satisfies a requirement to affirmatively specify security
exclude-all ::= &#39;!*&#39; [ &#39;::*&#39; ]
    ; all lower precedence tokens are ignored
    ; any lower precedence affirmation of security is ignored
exclude-scope ::= &#39;!&#39; scope-name [ &#39;::*&#39; ]
    ; all lower precedence tokens specified by the named scope are ignored
    ; any lower precedence affirmation of security resulting from the named scope is ignored
exclude-feature ::= &#39;!&#39; [ &#39;*&#39; ] &#39;::&#39; feature-name
    ; any lower precedence token specifying the named feature is ignored
    ; any lower precedence affirmation of security resulting from the named feature is ignored
exclude-feature-in-scope ::= &#39;!&#39; scope-name &#39;::&#39; feature-name
    ; any lower precedence token specifying the named feature and specified in the named scope is ignored
    ; any lower precedence affirmation of security resulting from the named scope and feature is ignored
suppression ::= ( suppress-all | suppress-feature )
suppress-all ::= &#39;NONE&#39;
    ; all lower precedence tokens are ignored
    ; the absence of security is affirmed
suppress-feature ::= feature-name &#39;:NONE&#39;
    ; any lower precedence token specifying the named feature is ignored
    ; any lower precedence affirmation of security resulting from the named feature is ignored
deferral ::= ( defer-all | defer-feature )
defer-all ::= &#39;DEFERRED&#39;
    ; the current security state, which may or may not be empty, is affirmed
defer-feature ::= feature-name &#39;:DEFERRED&#39;
    ; any lower precedence token specifying the named feature is ignored
    ; a new map entry is created that requires no security
    ; the security state is affirmed
assignment ::= ( assign-default-level | assign-default-feature | assign-feature-and-level )
assign-default-level ::= feature-name
    ; any lower precedence token specifying the named feature is ignored
    ; a new map entry is created that requires full access-level
    ; the security state is affirmed
assign-default-feature ::= &#39;:&#39; access-level
    ; any lower precedence token specifying the feature-name equivalent to &#39;\${service}Access&#39; is ignored
    ; a new map entry is created specifying the feature-name equivalent to &#39;\${service}Access&#39; and the given access level
    ; the security state is affirmed
assign-feature-and-level ::= feature-name &#39;:&#39; access-level
    ; any lower priority token specifying the named feature is made obsolete
    ; a new map entry is created specifying the named feature and the given access level
    ; the security state is affirmed
scope-name ::= ( &#39;DEFAULT&#39; | &#39;ESDLSERVICE&#39; | &#39;ESDLMETHOD&#39; | ... )
    ; additional names are anticipated resulting from binding integration
access-level ::= ( &#39;ACCESS&#39; | &#39;READ&#39; | &#39;WRITE&#39; | &#39;FULL&#39; )
reserved-word ::= scope-name | access-level
feature-name ::= ( feature-name-char* variable-ref-start variable-name &#39;}&#39; feature-name-char* | feature-name-char+ )
    ; feature-name-char is any char except whitespace, &#39;,&#39;, &#39;&quot;&#39;, &#39;!&#39;, or &#39;:&#39;
    ; feature-name has the added restrictions of not being a reserved-word
variable-name ::= ( &#39;SERVICE&#39; | &#39;METHOD&#39; )
variable-ref-start ::= ( &#39;\${&#39; | &#39;{$&#39; )
</code></pre>`,50)),d(t,{content:"%5B%7B%22type%22:%22js%22,%22exec%22:true,%22id%22:%22fence-1-1%22,%22content%22:%22service%22%7D%5D"})])}const M=i(s,[["render",c]]);export{y as __pageData,M as default};
