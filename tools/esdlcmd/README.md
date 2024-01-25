# esdl Utility

The esdl utility tool aids with creating and managing ESDL-based and Dynamic ESDL services on an HPCC cluster. It consists of several different commands.

To generate output from an ESDL definition:

    xml               Generate XML from ESDL definition.
    ecl               Generate ECL from ESDL definition.
    xsd               Generate XSD from ESDL definition.
    wsdl              Generate WSDL from ESDL definition.
    java              Generate Java code from ESDL definition.
    cpp               Generate C++ code from ESDL definition.
    monitor           Generate ECL code for result monitoring / differencing
    monitor-template  Generate a template for use with 'monitor' command

To manage ESDL and DESDL services:

    publish               Publish ESDL Definition for ESP use.
    list-definitions      List all ESDL definitions.
    get-definition        Get ESDL definition.
    delete                Delete ESDL Definition.
    bind-service          Configure ESDL based service on target ESP (with existing ESP Binding).
    list-bindings         List all ESDL bindings.
    unbind-service        Remove ESDL based service binding on target ESP.
    bind-method           Configure method associated with existing ESDL binding.
    unbind-method         Remove method associated with existing ESDL binding.
    get-binding           Get ESDL binding.
    manifest              Build a service binding or bundle from a manifest file.
    bind-log-transform    Configure log transform associated with existing ESDL binding.
    unbind-log-transform  Remove log transform associated with existing ESDL binding.

The sections below cover the commands in more detail.

## manifest

The `manifest` command creates an XML configuration file for an ESDL ESP from an input XML manifest file. The type of configuration output depends on the manifest file input and on command-line options.

### Manifest File

A manifest file is an XML-formatted template combining elements in and outside of the manifest's `urn:hpcc:esdl:manifest` namespace. Recognized elements of this namespace control the tool while all other markup is copied to the output. The goal of using a manifest file with the tool is to make configuring and deploying services easier:

1. The manifest file format abstracts some of the complexity of the actual configuration.
2. By allowing you to include external files like ESDL Scripts and XSLTs into the ouput, you can store and maintain them separately in your repo.

The result of running the manifest tool on a manifest file is an XML artifact suitable for use with the ESDL ESP. Supported output includes:

- `binding`: The output is an ESDL binding that may be published to dali.
- `bundle`: The output is an ESDL bundle file that may be used to launch an ESP in application mode.

### Example
A simplified example showing the format of a `bundle` manifest file:

```xml
<em:Manifest xmlns:em="urn:hpcc:esdl:manifest">
    <em:ServiceBinding esdlservice="WsFoobar" id="WsFoobar_desdl_binding" auth_feature="DEFERRED">
        <Methods>
            <em:Scripts>
                <em:Include file="WsFoobar-request-prep.xml"/>
                <em:Include file="WsFoobar-logging-prep.xml"/>
            </em:Scripts>
            <Method name="FoobarSearch" url="127.0.0.1:8888">
                <em:Scripts>
                    <em:Include file="FoobarSearch-scripts.xml"/>
                </em:Scripts>
            </Method>
        </Methods>
        <LoggingManager>
            <LogAgent transformSource="local" name="main-logging">
                <LogDataXPath>
                    <LogInfo name="PreparedData" xsl="log-prep">
                </LogDataXPath>
                <XSL>
                    <em:Transform name="log-prep">
                        <em:Include file="log-prep.xslt">
                    </em:Transform>
                </XSL>
            </LogAgent>
        </LoggingManager>
    </em:ServiceBinding>
    <em:EsdlDefinition>
        <em:Include file="WsFoobar.ecm"/>
    </em:EsdlDefinition>
</em:Manifest>
```
The tool is permissive and flexible, copying through most markup to the output. Recognized elements in the manifest namespace may be treated differently. They are only required in order to take advantage of the automated processing and simplified format of the manifest file. This example highlights the recommended usage of manifest elements to use the tool's capabilities. Although you could replace some of the elements below with verbatim `bundle` or `binding` output elements we won't cover that usage here.

* `<em:Manifest>` is the required root element. By default the tool outputs a `bundle`, though you may explicitly override that on the command line or by providing an `@outputType='binding'` attribute.
* `<em:ServiceBinding>` is valid for both `bundle` and `binding` output. It is necessary to enable recognition of `<em:Scripts>`, and `<em:Transform>` elements.
* `<em:EsdlDefinition>` is relevant only for `bundle` output. It is necessary to enable element order preservation and recognition of `<em:Include>` as a descendant element.
* `<em:Include>` causes external file contents to be inserted in place of the element. The processing of included files is context dependent; the parent of the `<em:Include>` element dictates how the file is handled. File inclusion facilitates code reuse in a configuration as code development environment.
* `<em:Scripts>` and `<em:Transform>` trigger preservation of element order for all descendent elements and enable `<em:Include>` recognition.

XML element ordering is significant to proper ESDL script, XSLT, and ESXDL content processing. The platform's `IPropertyTree` implementation, used when loading an artifact, does not preserve order. Output configuration files must embed order-sensitive content as text as opposed to XML markup. The tool allows configuration authors to create and maintain files as XML markup, which is easy to read. It then automates the conversion of the XML markup into the embedded text required by an ESP.

### Syntax

These elements may create artifact content, change the tool's behavior, or both. When used as intended, none of these elements will appear in the generated output:

#### Manifest
Required root element of all manifest files that create output:

- `bundle` output is created by default. It can be made explicit by setting either/or:
  - command line option `--output-type` to `bundle`
  - manifest property `@outputType` to `bundle`.
- `binding` output is created when either/or:
  - command line option `--output-type` is `binding`
  - manifest property `@outputType` is `binding`.

| Attribute | Required? | Value | Description |
| - | :-: | - | - |
| `@outputType` | N | string | A hint informing the tool which type of output to generate. The command line option `--output-type` may supersede this value to produce a different output.<br/>A `bundle` manifest is a superset of a `binding` manifest and may logically be used to create either output type. A `binding` manifest, as a subset of a `bundle`, cannot be used to create a valid `bundle`. |
| `@xmlns[:prefix]` | Y | string | The manifest namespace `urn:hpcc:esdl:manifest` must be declared. The default namespace prefix should not be used unless all other markup is fully qualified. |

#### ServiceBinding
Recommended child of `Manifest` that creates ESDL binding content and applies ESDL binding-specific logic to descendent content:

- A `Binding` output element is always created containing attributes of `Manifest` as required. See the sections below for details of the attributes referenced by the tool and how they're output.
- A child of `Binding` named `Definition` is created with attributes `@id` and `@esdlservice`.
- Recognition of `<em:Scripts>` elements is enabled.
- Recognition of `<em:Transform>` elements is enabled.

While possible to omit the `<em:ServiceBinding>` element and instead embed a complete `Binding` tree in the manifest, it is discouraged because you lose the benefit of the processing described above.

There are three categories of attributes that can be defined on the `<em:ServiceBinding>` element:

1. Standard attributes needed for setup
2. Service-specific or binding-specific attributes
3. Auxillary attributes recommended for read-only reference

##### Standard Attributes

First are the standard attributes used to setup and define the binding:

| Attribute   | Required? | Value  | Usage |
| - | :-: | - | - |
| `@esdlservice`  | Yes | string | - Name of the ESDL service to which the binding is bound. Output on the `Binding/Definition` element. <br/>- Also used to generate a value for `Definition/@id` for `bundle` type output. |

>Note that `Definition/@id` is not output by the tool for `binding` output as it would need to match the `ESDLDefinitionId` passed on command line `esdl bind-service` call, which may differ between environments.

##### Service-Specific Attributes

Second are binding-specific or service-specific attributes. This is an open-ended category where most future attributes will belong. Any attribute not in the other two categories is included here and output on the `Binding/Definition` element:

| Attribute   | Required? | Value  | Usage |
| - | :-: | - | - |
| `@auth_feature` | No | string | Used declare authorization settings if they aren't present in the ESDL Definition, or override them if they are. Additional documentation on this attribute is forthcoming. |
| `@returnSchemaLocationOnOK` | No | Boolean | When true, a successful SOAP response (non SOAP-Fault) will include the schema location property. False by default. |
| `@namespace` | No | string | String specifying the namespace for all methods in the binding. May contain variables that are replaced with their values by the ESP during runtime: <br/>- `${service}` : lowercase service name <br/>- `${esdl-service}` : service name, possibly mixed case <br/>- `${method}` : lowercase method name <br/>- `${esdl-method}` : method name, possibly mixed case <br/>- `${optionals}` : comma-delimited list of all optional URL parameters included in the method request, enclosed in parentheses <br/>- `${version}` : client version number |

##### Auxillary Attributes

Finally are auxillary attributes. These should be thought of as read-only or for reference, and it is not recommended that you set these in the manifest. They are set by the system when publishing to dali using `esdl bind-service`, and they are present on binding configurations retrieved from the dali. If you set these attributes in the manifest, the values will be overwritten when running `esdl bind-service`. Alternately, if you run as an esdl application, these values aren't set by default and don't have a material effect on the binding, but they may appear in the trace log.

| Attribute   | Required? | Value  | Usage |
| - | :-: | - | - |
| `@created`     | No | string | Timestamp of binding creation |
| `@espbinding`  | No | string | Set to match the `id`. Otherwise the value is unset and unused. |
| `@espprocess`  | No | string | Name of the ESP process this binding is running on |
| `@id`          | No | string | Runtime name of the binding. When publishing to dali the value is [ESP Process].[port].[ESDL Service]. When not present in the manifest a default value is generated of the form [@esdlservice]_desdl_binding|
| `@port`        | No | string | Port on the ESP Process listening for connections to the binding. |
| `@publishedBy` | No | string | Userid of the person publishing the binding |

#### EsdlDefinition
Recommended child of `<em:Manifest>` that enables ESDL definition-specific logic in the tool:

- A `Definitions` element is output. All content is enclosed in a CDATA section.
- The manifest `<em:Include>` element is recognized to import ESDL definitions:
  - An included .ecm file is transformed into XML.
  - An included .xml file is imported as-is.

| Attribute   | Required? | Value  | Usage |
| - | :-: | - | - |
| N/A |

The element is not required since it is possible to embed a complete `Definitions` element hierarchy in the manifest.

#### Include
Optional element that imports the contents of another file into the output in place of itself. The outcome of the import depends on the context in which this element is used. See [EsdlDefinition](#esdldefinition), [Scripts](@scripts), and [Transform](#transform) for more information.

| Attribute   | Required? | Value  | Usage |
| - | :-: | - | - |
| `@file` | Yes | file path | Full or partial path to an external file to be imported. If a partial path is outside of the tool's working directory, the tool's command line must specify the appropriate root directory using either `-I` or `--include-path`. |

>Any XSLTs or ESDL Scripts written inline in a manifest file will have XML escaping applied where required to generate valid XML. If an XSLT contains any text content or markup that needs to be preserved as-is (no XML escaping applied) then be sure to use an `<em:Include>` operation. Included files are inserted into the output as-is, with the exception of encoding nested CDATA markup. If the included file will be inside a CDATA section on output, then any CDATA end markup in the file will be encoded as `]]]]><![CDATA[>` to prevent nested CDATA sections or a prematurely ending a CDATA section.
>
>For details on using XSLT to generate unescaped output, see this section of the specification: https://www.w3.org/TR/1999/REC-xslt-19991116#disable-output-escaping

#### Scripts
Optional repeatable element appearing within an `<em:ServiceBinding>` element that processes child elements and creates output expected for an ESDL binding:

- The `<em:Scripts>` element is replaced on output with `<Scripts>`. Then all content is enclosed in a CDATA section after wrapping it with a new `Scripts` element. That new `<Scripts>` element contains namespaces declared by the input `<em:Scripts>` element. The input `<em:Scripts foo="..." xmlns:bar="..."><!-- content --></em:Scripts>` becomes `<Scripts><![CDATA[<Scripts xmlns:bar="..."><!-- content --></Scripts>]]></Scripts>`.
- The manifest `<em:Include>` element is recognized to import scripts from external files. The entire file, minus leading and trailing whitespace, is imported. Refrain from including files that contain an XML declaration.

#### Transform
Optional repeatable element appearing within an `<em:ServiceBinding>` element that processes child elements and creates output expected for an ESDL binding:

- All content is enclosed in a CDATA section. The input `<em:Transform> <!-- content --> </em:Transform>` becomes `<Transform><![CDATA[<!-- content -->]]></Transform>`.
- The manifest `<em:Include>` element is recognized to import transforms from external files. The entire file, minus leading and trailing whitespace, is imported. Refrain from including files that contain an XML declaration.

### Usage

    Usage:

    esdl manifest <manifest-file> [options]

    Options:
        -I | --include-path <path>
                            Search path for external files included in the manifest.
                            Use once for each path.
        --outfile <filename>
                            Path and name of the output file
        --output-type <type>
                            When specified this option overrides the value supplied
                            in the manifest attribute Manifest/@outputType.
                            Allowed values are 'binding' or 'bundle'.
                            When not specified in either location the default is
                            'bundle'
        --help              Display usage information for the given command
        -v,--verbose        Output additional tracing information
        -tcat,--trace-category <flags>
                            Control which debug messages are output; a case-insensitive
                            comma-delimited combination of:
                                dev: all output for the developer audience
                                admin: all output for the operator audience
                                user: all output for the user audience
                                err: all error output
                                warn: all warning output
                                prog: all progress output
                                info: all info output
                            Errors and warnings are enabled by default if not verbose,
                            and all are enabled when verbose. Use an empty <flags> value
                            to disable all.

### Output

The esdl `manifest` command reads the manifest, processes statements in the `urn:hpcc:esdl:manifest` namespace and generates an output XML file formatted to the requirements of the ESDL ESP. This includes wrapping included content in CDATA sections to ensure element order is maintained and replacing `urn:hpcc:esdl:manifest` elements as required.

An example output of each type -`bundle` and `binding`- is shown below. The examples use the sample manifest above as input plus these included files:

**WsFoobar-request-prep.xml**

```xml
<es:BackendRequest name="request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
    <es:set-value target="RequestValue" value="'foobar'"/>
</es:BackendRequest>
```

**WsFoobar-logging-prep.xml**

```xml
<es:PreLogging name="logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
    <es:set-value target="LogValue" value="23"/>
</es:PreLogging>
```

**FoobarSearch-scripts.xml**

```xml
<Scripts>
    <es:BackendRequest name="search-request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
        <es:if test="RequestOption>1">
            <es:set-value target="HiddenOption" value="true()"/>
        </es:if>
    </es:BackendRequest>

    <es:PreLogging name="search-logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
        <es:if test="RequestOption=1">
            <es:set-value target="ProductPrice" value="10"/>
        </es:if>
    </es:PreLogging>
</Scripts>
```

**log-prep.xslt**

```xml
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" omit-xml-declaration="yes"/>
    <xsl:variable name="logContent" select="/UpdateLogRequest/LogContent"/>
    <xsl:variable name="transactionId" select="$logContent/UserContext/Context/Row/Common/TransactionId"/>
    <xsl:template match="/">
        <Result>
        <Dataset name='special-data'>
            <Row>
            <Records>
                <Rec>
                <transaction_id><xsl:value-of select="$transactionId"/></transaction_id>
                <request_data>
                    <xsl:text disable-output-escaping="yes">&amp;lt;![CDATA[COMPRESS('</xsl:text>
                    <xsl:copy-of select="$logContent/UserContent/Context"/>
                    <xsl:text disable-output-escaping="yes">')]]&amp;gt;</xsl:text>
                </request_data>
                <request_format>SPECIAL</request_format>
                <type>23</type>
                </Rec>
            </Records>
            </Row>
        </Dataset>
        </Result>
    </xsl:template>
</xsl:stylesheet>
```

**WsFoobar.ecm**

    ESPrequest FoobarSearchRequest
    {
        int RequestOption;
        string RequestName;
        [optional("hidden")] bool HiddenOption;
    };

    ESPresponse FoobarSearchResponse
    {
        int FoundCount;
        string FoundAddress;
    };

    ESPservice [
        auth_feature("DEFERRED"),
        version("1"),
        default_client_version("1"),
    ] WsFoobar
    {
        ESPmethod FoobarSearch(FoobarSearchRequest, FoobarSearchResponse);
    };

#### Bundle

The bundle is suitable to configure a service on an ESP launched in esdl application mode.

```xml
  <EsdlBundle>
    <Binding id="WsFoobar_desdl_binding">
      <Definition esdlservice="WsFoobar" id="WsFoobar.1">
        <Methods>
          <Scripts>
            <![CDATA[
              <Scripts>
                <es:BackendRequest name="request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                    <es:set-value target="RequestValue" value="'foobar'"/>
                </es:BackendRequest>
                <es:PreLogging name="logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                    <es:set-value target="LogValue" value="23"/>
                </es:PreLogging>
              </Scripts>
            ]]>
          </Scripts>
          <Method name="FoobarSearch" url="127.0.0.1:8888">
            <Scripts>
              <![CDATA[
                <Scripts>
                  <Scripts>
                      <es:BackendRequest name="search-request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                          <es:if test="RequestOption>1">
                              <es:set-value target="HiddenOption" value="true()"/>
                          </es:if>
                      </es:BackendRequest>

                      <es:PreLogging name="search-logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                          <es:if test="RequestOption=1">
                              <es:set-value target="ProductPrice" value="10"/>
                          </es:if>
                      </es:PreLogging>
                  </Scripts>
                </Scripts>
              ]]>
            </Scripts>
          </Method>
        </Methods>
        <LoggingManager>
          <LogAgent transformSource="local" name="main-logging">
            <LogDataXPath>
              <LogInfo name="PreparedData" xsl="log-prep"/>
            </LogDataXPath>
            <XSL>
              <Transform name="log-prep">
                <![CDATA[
                  <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                      <xsl:output method="xml" omit-xml-declaration="yes"/>
                      <xsl:variable name="logContent" select="/UpdateLogRequest/LogContent"/>
                      <xsl:variable name="transactionId" select="$logContent/UserContext/Context/Row/Common/TransactionId"/>
                      <xsl:template match="/">
                          <Result>
                          <Dataset name='special-data'>
                              <Row>
                              <Records>
                                  <Rec>
                                  <transaction_id><xsl:value-of select="$transactionId"/></transaction_id>
                                  <request_data>
                                      <xsl:text disable-output-escaping="yes">&amp;lt;![CDATA[COMPRESS('</xsl:text>
                                      <xsl:copy-of select="$logContent/UserContent/Context"/>
                                      <xsl:text disable-output-escaping="yes">')]]&amp;gt;</xsl:text>
                                  </request_data>
                                  <request_format>SPECIAL</request_format>
                                  <type>23</type>
                                  </Rec>
                              </Records>
                              </Row>
                          </Dataset>
                          </Result>
                      </xsl:template>
                  </xsl:stylesheet>
                ]]>
              </Transform>
            </XSL>
          </LogAgent>
        </LoggingManager>
      </Definition>
    </Binding>
    <Definitions>
      <![CDATA[
        <esxdl name="WsFoobar"><EsdlRequest name="FoobarSearchRequest"><EsdlElement  type="int" name="RequestOption"/><EsdlElement  type="string" name="RequestName"/><EsdlElement  optional="hidden" type="bool" name="HiddenOption"/></EsdlRequest>
        <EsdlResponse name="FoobarSearchResponse"><EsdlElement  type="int" name="FoundCount"/><EsdlElement  type="string" name="FoundAddress"/></EsdlResponse>
        <EsdlRequest name="WsFoobarPingRequest"></EsdlRequest>
        <EsdlResponse name="WsFoobarPingResponse"></EsdlResponse>
        <EsdlService version="1" auth_feature="DEFERRED" name="WsFoobar" default_client_version="1"><EsdlMethod response_type="FoobarSearchResponse" request_type="FoobarSearchRequest" name="FoobarSearch"/><EsdlMethod response_type="WsFoobarPingResponse" auth_feature="none" request_type="WsFoobarPingRequest" name="Ping"/></EsdlService>
        </esxdl>
      ]]>
    </Definitions>
  </EsdlBundle>
  ```

#### Binding

The binding can be used to configure a service for an ESP using a dali.

```xml
<Binding id="WsFoobar_desdl_binding">
  <Definition esdlservice="WsFoobar" id="WsFoobar.1">
    <Methods>
      <Scripts>
        <![CDATA[
          <Scripts>
            <es:BackendRequest name="request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                <es:set-value target="RequestValue" value="'foobar'"/>
            </es:BackendRequest>
            <es:PreLogging name="logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                <es:set-value target="LogValue" value="23"/>
            </es:PreLogging>
          </Scripts>
        ]]>
      </Scripts>
      <Method name="FoobarSearch" url="127.0.0.1:8888">
        <Scripts>
          <![CDATA[
            <Scripts>
              <Scripts>
                  <es:BackendRequest name="search-request-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                      <es:if test="RequestOption>1">
                          <es:set-value target="HiddenOption" value="true()"/>
                      </es:if>
                  </es:BackendRequest>

                  <es:PreLogging name="search-logging-prep" target="soap:Body/{$query}" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
                      <es:if test="RequestOption=1">
                          <es:set-value target="ProductPrice" value="10"/>
                      </es:if>
                  </es:PreLogging>
              </Scripts>
            </Scripts>
          ]]>
        </Scripts>
      </Method>
    </Methods>
    <LoggingManager>
      <LogAgent transformSource="local" name="main-logging">
        <LogDataXPath>
          <LogInfo name="PreparedData" xsl="log-prep"/>
        </LogDataXPath>
        <XSL>
          <Transform name="log-prep">
            <![CDATA[
              <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                  <xsl:output method="xml" omit-xml-declaration="yes"/>
                  <xsl:variable name="logContent" select="/UpdateLogRequest/LogContent"/>
                  <xsl:variable name="transactionId" select="$logContent/UserContext/Context/Row/Common/TransactionId"/>
                  <xsl:template match="/">
                      <Result>
                      <Dataset name='special-data'>
                          <Row>
                          <Records>
                              <Rec>
                              <transaction_id><xsl:value-of select="$transactionId"/></transaction_id>
                              <request_data>
                                  <xsl:text disable-output-escaping="yes">&amp;lt;![CDATA[COMPRESS('</xsl:text>
                                  <xsl:copy-of select="$logContent/UserContent/Context"/>
                                  <xsl:text disable-output-escaping="yes">')]]&amp;gt;</xsl:text>
                              </request_data>
                              <request_format>SPECIAL</request_format>
                              <type>23</type>
                              </Rec>
                          </Records>
                          </Row>
                      </Dataset>
                      </Result>
                  </xsl:template>
              </xsl:stylesheet>
            ]]>
          </Transform>
        </XSL>
      </LogAgent>
    </LoggingManager>
  </Definition>
</Binding>
```
