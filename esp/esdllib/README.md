# ESDL Scripting

ESDL Script is a highly domain specific proprietary scripting language and engine built into the ESP. The engine's close integration with the ESP allows it to expose scripting Entry Points at important steps in the lifetime of an ESP transaction. Each Entry Point presents an opportunity to run scripts that modify the context holding the transaction state, passing the modified context to the next transaction step.

The markup syntax of the scripting language is XML based and purpose-built for scripting the business logic needed in ESP services. These three features make it well suited to that task:

- Most Entry Point scripts operate on a copy of their expected output, so you only need to make the minimum modifications necessary instead of building up the entire output from scratch.
- It has operations that help with common ESP transaction logic, like running MySQL queries, making SOAP calls, input validation, and checking a user's authorization status.
- ESDL Scripts are parsed and transformed into execution graphs at startup, then the execution graphs are executed at runtime. Your ESDL Script business logic is isolated from the compiled C++ code of the platform, and can be separately managed and updated.

## History

ESDL started as a way to standardize and streamline service and microservice development, and has seen continual improvement to support more complex uses. Its foundational features let us stand up a front end service for a roxie backend with just an interface description and no other customized logic or coding. Starting there we have the benefit of robust interfaces isolated from the compiled code of the HPCC Platform itself. You can easily update a service's interface or its configuration binding it to the backend without building or deploying any new C++ code.

The desire to use ESDL for more services brought plugin support for customized logging and authentication. However, even with logging and authentication plugins available we saw the need for customization and logic beyond the standard transaction steps of "authenticate, call backend then log". To support this customization and logic we've implemented the ESDL Scripting language with an engine that hooks into various steps of the transaction.

This document is an overview and reference of the ESDL Scripting language.

## Table of Contents

<!-- TOC -->

- [ESDL Scripting](#esdl-scripting)
  - [History](#history)
  - [Table of Contents](#table-of-contents)
- [Language Overview](#language-overview)
  - [Basic Example](#basic-example)
  - [Transaction Flow and Entry Points](#transaction-flow-and-entry-points)
  - [Scripting Context](#scripting-context)
  - [Configuration](#configuration)
    - [ESP Launch Mode Influences Configuration Format](#esp-launch-mode-influences-configuration-format)
    - [Service Bundle Configuration Commonalities](#service-bundle-configuration-commonalities)
    - [Application Mode](#application-mode)
    - [Legacy Mode](#legacy-mode)
    - [Debugging](#debugging)
    - [Logging](#logging)
    - [Service Security](#service-security)
- [Reference](#reference)
  - [Conventions](#conventions)
    - [Implicit Variables](#implicit-variables)
    - [System Parameters](#system-parameters)
  - [Entry Points](#entry-points)
    - [BackendRequest](#backendrequest)
    - [BackendResponse](#backendresponse)
    - [EsdlResponse](#esdlresponse)
    - [PreLogging](#prelogging)
    - [Service](#service)
  - [XPath](#xpath)
    - [Variable Resolution](#variable-resolution)
      - [Location Paths and Predicates](#location-paths-and-predicates)
      - [Expressions and Function Arguments](#expressions-and-function-arguments)
    - [Extensions](#extensions)
    - [Namespaces](#namespaces)
      - [soap Example](#soap-example)
      - [Default Namespace Examples](#default-namespace-examples)
    - [EXSLT Functions](#exslt-functions)
    - [Custom Functions](#custom-functions)
      - [ensureDataSection](#ensuredatasection)
        - [Example](#example)
      - [getDataSection](#getdatasection)
        - [Example](#example-1)
      - [getFeatureSecAccessFlags](#getfeaturesecaccessflags)
      - [getLogOption](#getlogoption)
      - [getLogProfile](#getlogprofile)
      - [getStoredStringValue](#getstoredstringvalue)
      - [logOptionExists](#logoptionexists)
      - [secureAccessFlags](#secureaccessflags)
      - [storedValueExists](#storedvalueexists)
      - [tokenize](#tokenize)
      - [Example](#example-2)
      - [validateFeaturesAccess](#validatefeaturesaccess)
  - [Operations](#operations)
    - [add-value](#add-value)
    - [append-to-value](#append-to-value)
    - [assert](#assert)
    - [choose](#choose)
    - [copy-of](#copy-of)
    - [element](#element)
    - [ensure-target](#ensure-target)
    - [fail](#fail)
    - [for-each](#for-each)
    - [http-post-xml](#http-post-xml)
      - [Example](#example-3)
    - [if](#if)
    - [if-source](#if-source)
    - [if-target](#if-target)
    - [mysql](#mysql)
      - [Credentials](#credentials)
      - [Examples](#examples)
    - [namespace](#namespace)
    - [param](#param)
    - [remove-node](#remove-node)
    - [rename-node](#rename-node)
    - [set-log-option](#set-log-option)
    - [set-log-profile](#set-log-profile)
    - [set-value](#set-value)
    - [source](#source)
    - [store-value](#store-value)
    - [target](#target)
    - [variable](#variable)

<!-- /TOC -->

# Language Overview

The ESDL Script language consists of XML elements and XPath functions. A script is an ordered collection of XML elements that may use XPath functions. Each element is either an *entry point* or an *operation*.

An *entry point* element is named after a step in the ESDL transaction workflow. It is the root node of a list of ordered *operations*. Each *entry point* may have multiple *entry point* elements. The *operations* are defined in the `urn:hpcc:esdl:script` namespace. As the ESP processes a transaction, it steps into each defined script *entry point* in turn and executes each *entry point* element in order. Likewise, each *entry point* element executes its *operations* in order.

Each entry point has default *source* and *target* XML nodes. Most *entry points* start by copying the *source* through to the *target*, with exceptions noted in the [Entry Points reference](#entry-points). Default *operation* behavior reads from the *source* and applies changes to the *target*.

Most services contain methods that call out to a backend server such as a roxie. However, a service method can be written purely in ESDL Script without calling to a backend server. There is a special entry point used in this case that must construct the resulting target XML node. Refer to the [Entry Points](#entry-points) reference for more details on all entry points.

As the operations are executed in each entry point, the ESDL Script engine maintains an *ESDL Script context* that holds state between operations, including elements corresponding to the *source* and *target* nodes of the entry points, and other nodes created by other ESDL Script operations. The [Scripting Context](#scripting-context) section has additional information.

ESDL Scripts are written in XML and share some similarites with XSLT, but are easier to understand and have some powerful new features.

**XSLT Similarities:**

How is ESDL Script like XSLT?

1. Written in XML
2. Use XPaths and support standard, extended and custom XPath functions.
3. Variables can contain scalar or node-set values

**XSLT Differences:**

How is ESDL Script different from XSLT?

1. In most cases you don't construct a result from scratch, you _transform_ an existing element into your desired result. This gives sensible default behavior with no scripting and keeps required scripts smaller.
2. Scripts do not use the often difficult-to-trace template matching behavior of xslt, they are procedural- executing operations in order.
3. There is no text content like in XSLT.

## Basic Example

A very common use case for scripts is transforming the incoming request to the ESP into a format required by a backend roxie query. When ESDL is used the interfaces will match, but customization can be required for legacy services, or when business logic demands that the request be modified in some way. This example is an introduction to the ESDL Script language and doesn't show the input validation and error handling needed in a production-ready script.

Say we have an incoming ESP request in this format:

```xml
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="urn:hpccsystems:ws:wssample:testmethod">
  <soap:Body>
    <TestMethodRequest>
      <Options>
        <StartDate>
          <Year>1976</Year>
          <Month>07</Month>
          <Day>12</Day>
        </StartDate>
      </Options>
      <SearchBy>
        <Name>
          <First>Jon</First>
          <Last>Jonson</Last>
        </Name>
      </SearchBy>
    </TestMethodRequest>
  </soap:Body>
</soap:Envelope>
```

When using ESDL in front of a roxie query, there is a built in default transformation which produces a call to roxie that looks something like this (Sample 1):

```xml
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <TestModule.TestMethodService>
      <Context>...etc...</Context>
      <TestMethodRequest>
        <Row>
          <Options>
            <StartDate>
              <Year>1976</Year>
              <Month>07</Month>
              <Day>12</Day>
            </StartDate>
          </Options>
          <SearchBy>
            <Name>
              <First>Jon</First>
              <Last>Jonson</Last>
            </Name>
          </SearchBy>
        </Row>
      </TestMethodRequest>
    </TestModule.TestMethodService>
  </soap:Body>
</soap:Envelope>
```

However, let's say the backend query expects a few differences -
1. StartDate is in YYYYMMDD format
2. Options must include the `ExtendedSearchEnable` Secure User Setting (from the Authorization results of the Security Manager plugin)

Then the backend request must look like this (Sample 2):

```xml
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <TestModule.TestMethodService>
      <Context>...etc...</Context>
      <TestMethodRequest>
        <Row>
          <Options>
            <StartDate>19760712</StartDate>
            <ExtendedSearchEnable>1<ExtendedSearchEnable>
          </Options>
          <SearchBy>
            <Name>
              <First>Jon</First>
              <Last>Jonson</Last>
            </Name>
          </SearchBy>
        </Row>
      </TestMethodRequest>
    </TestModule.TestMethodService>
  </soap:Body>
</soap:Envelope>
```

A script to make this change could be coded like this:

```xml
<es:BackendRequest name="TransformForRoxie" target="soap:Body/{$query}/{$request}" xmlns:es="urn:hpcc:esdl:script">
  <es:param name="ExtendedSearchEnable" value="0"/>
  <es:source xpath="soap:Body/*/*/Row/Options/StartDate">
    <es:set-value target="Options/StartDate" select="Year"/>
    <es:append-to-value target="Options/StartDate" select="Month"/>
    <es:append-to-value target="Options/StartDate" select="Day"/>
  </es:source>
  <es:set-value target="Options/ExtendedSearchEnable" select="{$ExtendedSearchEnable}"/>
</es:BackendRequest>
```

Let's discuss what each line of this script does.

```xml
<es:BackendRequest name="TransformForRoxie" target="soap:Body/{$query}/{$request}" xmlns:es="urn:hpcc:esdl:script">
```

This script consists of a single *entry point*, `BackendRequest`, that is executed after the ESDL transformer has generated the default backend request just before it is sent to the backend.

The scripting engine setup for most entry points, including `BackendRequest`, copies the _source_ to the _target_. In this example the _source_ and _target_ start out looking like **Sample 1**. By the time script is completed the _target_ is **Sample 2**.

In cases like this, the action of the script can be thought of like the assignment of a variable: `x = Fn(x)`. The Left Hand Side **x** is modified by the result of the expression on the Right Hand Side **Fn(x)**. The Left Hand Side, or LHS is called the _target_ and the Right Hand Side or RHS is called the _source_.

The `@name` attribute is used for tracing and error reporting. The `@target` attribute is an XPath specifying the default root node of the target for all subsequent operations. If it is not present the root is the node `<soap:Envelope>`. The `{$query}` and `{$request}` variables are special system macros that are set to the name of the backend query and request respectively.

The `@xmlns` attribute is the standard XML definition of a namespace prefix. For ESDL Scripts the required namespace URN is `urn:hpcc:esdl:script`. You can define your own prefix (we use `es`), but the namespace prefixes and definitions should be consistent across all scripts in a service.

```xml
<es:param name="ExtendedSearchEnable" value="0"/>
```

This line defines a parameter named `ExtendedSearchEnable` with a default value of zero `0`. The `<es:param>` operation allows the script to make use of one of the values made available by the scripting engine. Setting the  `@value` attribute ensures that will be the default value if the parameter is unset. This avoids triggering an error by referencing an undefined parameter.

Assume this parameter is from a _Secure User Property_ assigned a value by the security manager plugin. The ESDL Script engine makes all the Secure User Properties set by the secmgr available to scripts as parameters.

```xml
<es:source xpath="soap:Body/*/*/Row/Options/StartDate">
```

This modifies the default root node of the source of all child operations. The `@xpath` attribute speficies the node to make the default source root node.

```xml
<es:set-value target="Options/StartDate" select="Year"/>
```

This sets the value of the target node specified by `@target` to the value of the node specified by `@source`. Any previous value of `@target` is replaced by the new value of `@source`. Because this command is a child of the previous `<es:source>` command, the `@select` is relative to `soap:Body/*/*/Row/Options/StartDate`

```xml
<es:append-to-value target="Options/StartDate" select="Month"/>
<es:append-to-value target="Options/StartDate" select="Day"/>
```

These both append the value of the `@select` node to the value of the `@target` node.

## Transaction Flow and Entry Points

The lifecycle of an ESP transaction is shown below in the flowchart's plain black boxes. Optionally, scripts can be inserted at any of the entry points named in the light grey boxes, and an entry point is skipped if it contains no scripts. At each entry point the script has the opportunity to transform the source in preparation for passing it on to the next transaction step.


``` mermaid
flowchart TD
    classDef transactionStep fill:#000;
    classDef entryPoint fill:#444;
    start(( ))-->receive[Receive request]
    receive-->esdl1[ESDL transforms ESP request \n to backend request]
    esdl1-->isScript{Fully Scripted-\nno backend?}

    isScript--no-->backendRequest[[BackendRequest:\nTransform backend request\nto final form]]
    backendRequest-->callBackend[Call backend]
    callBackend-->backendResponse[[BackendResponse: \nTransform backend response \n to raw response]]
    backendResponse-->esdl2[ESDL transforms raw response\n to initial ESP response]

    isScript--yes-->service[[Service: \nRun script as backend,\n create raw response]]
    service-->esdl2

    esdl2-->needEsdl3{EsdlResponse \n script \n present?}

    needEsdl3--no-->send[Send\nResponse]
    needEsdl3--yes-->esdlResponse[[EsdlResponse: \n Transform initial ESP response \n to final ESP response]]

    esdlResponse-->esdl3[ESDL transforms final ESP \nresponse to match schema]
    esdl3-->send
    send-->preLogging[[PreLogging: \n Transform Log Request]]

    preLogging-->queueLog[Log Manager dispatches Log Request]
    class start,receive,esdl1,isScript,callBackend,esdl2,esdl3,queueLog transactionStep;
    class backendRequest,service,backendResponse,esdlResponse,preLogging entryPoint;
```

A script is inserted to an entry point by naming its root node after the name of the entry point. A script can replace a backend service call by configuring a method's `@querytype` to script.

Additional details about the [entry points](#entry-points) and script [configuration](#configuration) are found in the reference section.

## Scripting Context

The scripting engine maintains a context during the lifetime of each transaction. This context holds state for the scripts, and the results of executing the operations at each entry point. The context contains an XML document holding most of the state that script operations can inspect and modify.

Below is a first-level outline of the common sections in the XML context:

```xml
<esdl_script_context>
    <esdl/>
    <original_request/>
    <target/>
    <config/>
    <esdl_request/>
    <final_request/>
    <initial_response/>
    <pre_esdl_response/>
    <logdata/>
    <logging/>
    <store/>
    <temporaries/>
</esdl_script_context>
```

Context Node | Contents
-------------|-------------------
`<esdl_script_context>` | Root node, should not be accessed by scripts
`<esdl>`                | ESDL interface definition info about the current method and service in progress.
`<original_request>`    | The unmodified request as the ESP recieved from the client.
`<target>`              | Configuration of the backend target from the ESDL service binding.
`<config>`              | The `EspBinding` section from the ESDL service binding. This contains any defined security manager Authenticate configuration.
`<esdl_request>`        | The result of sending the `original_request` through the esdl request transformer.
`<final_request>`       | The result of any transformations the `BackendRequest` entry point has made on the `esdl_request` node.
`<initial_response>`    | The unmodified response from the backend server, frequently a roxie.
`<pre_esdl_response>`   | The result of any transformations the `BackendResponse` entry point has made on the `initial_response` node.
`<logdata>`             | This node is passed on to the LoggingManager. The default source and target location of the `PreLogging` entry point.
`<logging>`             | Log option and log profile settings are held here and passed on to the LoggingManager.
`<store>`               | Location to store persistent values during the lifespan of the transaction.
`<temporaries>`         | Location where temporaries and other variables are created by default.

Some script operations will add additional elements to the context document.

## Configuration

In order to understand how ESDL Scripts are associated with an ESDL service we'll look at an overview of how services are configured. This is not an exhaustive reference, but shows where ESDL Scripts are in a configuration and gives some context of the other main configuration sections. These are our best practice recommendations for configuration. Other approaches are possible, but they aren't covered here.

An ESDL service is configured from two sources- the *process configuration* and the *service bundle configuration*.

The *process configuration* controls settings such as:

- An ESP process for the service to run inside of
- Which authentication method and plugin is used for the service
- TLS settings
- Trace log settings including transaction summary lines
- Dali server settings
- Authentication and authorization *Feature* and *Setting* entries

The *service bundle configuration* controls settings such as:

- The ESDL interface of the service
- Configuration of each *method* in the service including the backend target and any other arbitrary information the method requires
- ESDL Scripts customizing behavior of the service and its methods
- The *LoggingManager* and its associated *LogAgents*
- XSLT scripts used to prepare a logging payload, called *Log Transforms*
- *Feature* authorization requirements

The *process configuration* defines a running ESP process that loads and handles transactions from one or more ESDL services described in *service bundle configurations*.

### ESP Launch Mode Influences Configuration Format

The format of these configurations and how they're managed depends on how the ESP is launched. First is launching an ESP executable directly in *application mode* starting with an `esp.yaml` *process configuration*. Second is *legacy mode* launch where the platform's `environment.xml` *process configuration* file is used by SystemV or systemd startup scripts to launch an ESP.

First we'll explain some common sections of the *service bundle configuration* that apply to both launch modes. Then the following launch mode sections compare configuration hilights of the same service for both *application mode* and for *legacy configuration mode*.

Say we have a service named *WsMyService* that uses hypothetical authentication and logging plugins named *mypluginauth* and *my_logagent*. The service uses TLS and is exposed on port 8880. The directions and configuration examples below show the main sections for common settings but they don't go into detail or cover all possible settings.

### Service Bundle Configuration Commonalities

Regardless of launch mode, *service bundle configuration* contains a `<Binding>` element which may hold these settings:

```xml
<Binding espprocess="MyEspInstance" espbinding="" id="">
  <Definition esdlservice="WsMyService" id="WsMyService.1" auth_feature="">
    <Methods>
      <Scripts>
        <![CDATA[<Scripts>...ESDL Script entry points for all methods...</Scripts>]]>
      </Scripts>
      <Method name="" url="" auth_feature="" traceLevel="">
        <Scripts>
          <![CDATA[<Scripts>...ESDL Script entry points for this method...</Scripts>]]>
        </Scripts>
        ...arbitrary configuration elements...
      </Method>
      ...additional Methods...
    </Methods>
    <LoggingManager name="my_service_logman">
      ...LogManager configuration...
      <LogAgent name="my_logagent" type="" services="">
        ...LogAgent configuration...
        <XSL>
          <Transform>
            <![CDATA[<xsl:stylesheet>...XSLT...</xsl:stylesheet>]]>
          </Transform>
        </XSL>
      </LogAgent>
      ...additional LogAgents...
    </LoggingManager>
  </Definition>
</Binding>
```

Each `<Scripts>` element contains ESDL Script entry points that are executed as the transaction proceeds through its lifetime. Scripts in the `<Methods>` section are referred to as *service-level scripts*. They are executed by each method in a service. Scripts in each `<Method>` section are referred to as *method-level scripts* and are only executed when that method is run.

As the ESP processes a Method request and reaches each script entry point, it executes all of that entry point's scripts. The service-level scripts execute first, in order, followed by the method-level scripts in order. The CDATA sections ensure that the order of operations within each entry point's script is preserved.

ESDL service methods must have a `<Method>` element in the *service bundle config* to be invoked. The `@auth_feature` attribute on the `<Definition>` or `<Method>` elements specify security as described in the [Security Readme](https://github.com/hpcc-systems/HPCC-Platform/master/esp/esdllib/README-SECURITY.md#esdl-binding).

The `<LoggingManager>` section contains settings for the service's logging. The `<LogAgent>` elements may contain `XSL/Transform` elements used to customize the outgoing logging request before it is dispatched.

It is easiest to manage the ESDL Scripts, ESDL definitions and XSLT transforms that make up the *service bundle configuration* as separate files under source control. An upcoming feature of the `esdl` tool is a command to compile a *service bundle configuration* from these separate files.

### Application Mode

The Application Mode ESP deployment is designed for containerized cloud deployments, but can also work on bare metal. Part of what makes it so well suited to cloud deployments is that the ESP loads all configuration from files. An ESP can be launched as one of several different kinds of applications including `eclwatch` and `esdl`. We will only consider the *esdl application* here.

An *esdl application* configuration starts with an `esp.yaml` *process configuration* file, though in cloud deployments it would be called a *configmap*. Each application mode has reasonable default values for all settings, so your file need only include customized settings. Refer to the default application mode configuration files in the [HPCCSystems repository](https://github.com/hpcc-systems/HPCC-Platform/tree/master/esp/applications/esdl) for some examples. Note that the helm installation procedure includes a step that rolls up all the separate yaml files into `esp.yaml` prior to launch.

A pared-down example of an `esp.yaml` file may look like this:

```yaml
esp:
  instance: MyEspInstance
  description: ESDL Application for WsMyService
  daliServers: none
  loadDaliBindings: false
  auth: mypluginauth
  authDomain:
    # ... other settings
  tls: true
  service:
    port: 8880
  bindings: "/opt/HPCCSystems/bindings"
  # ... other settings

  bindingInfo:
    esdl:
      desdlBinding:
        resource_map:
          WsMyService:
            Feature:
            - access: Read
              authenticate: Yes
              path: AllowMyServiceExampleMethod
              resource: AllowMyServiceExampleMethod
              required: ''
              description: Controls access to the ExampleMethod of WsMyService
            Setting:
            - path: PremiumUserLevel
              resource: 'Premium User Level'
              description: Value 1-3 indicating allowed level of premium features

  tls_config:
    certificate: # ...
    privatekey: # ...
    # ... other settings

  authNZ:
    mypluginauth:
      SecurityManager:
        type: MyPluginSecmgrType
        name: mysecmgr
      root_access:
        resource: EnableSOAPInterface
        required: Read
        description: Root access to service
      # ... other settings
```

The chart below explains the meaning of the most important settings in the above example:

| Setting          | Meaning                                     |
|------------------|---------------------------------------------|
| daliServers      | Name of dali server to use, or none to not use dali |
| loadDaliBindings | If false do not load service bundle configuration from dali |
| tls              | If true use secure TLS connections |
| bindings         | Path to all the ESDL *service bundle configurations* that this process will load and bind to listen on service.port |
| service.port     | Port number to connect to bound esdl services |
| bindingInfo.esdl.desdlBinding.resource_map.WsMyService | Contains list of Feature and Setting elements that define the security resources for WsMyService using the mypluginauth security manager |
| tls_config       | Element containing all the TLS settings |
| authNZ.mypluginauth | Security manager plugin settings for the mypluginauth plugin |

Now say we have a *service bundle configuration* named `WsMyService.xml` located at the `esp.bindings` path of `/opt/HPCCSystems/bindings`. In outline, the service bundle config would look like this:

```xml
<EsdlBundle>
    <Binding espprocess="MyEspInstance" espbinding="" id="">
        <Definition esdlservice="WsMyService" id="WsMyService.1" name="">
          ... contents described in section 'Service Bundle Configuration Commonalities'
        </Definition>
    </Binding>
    <Definitions>
        <![CDATA[<esxdl name="WsMyService">...ESDL interface definition...</esxdl>]]>
    </Definitions>
</EsdlBundle>
```
For *esdl application* launches, the *service bundle configuration* contains an additional `<Definitions>` element which holds the ESDL definition of the service.

There are a few naming constraints that must be honored in the service bundle config and the process config. If the name of the ESDL interface is *WsMyService* (`EsdlBundle/Definitions/esxdl[@name] = WsMyService]`) that means these attributes must be named as shown:

- `EsdlBundle/Binding/Definition[@esdlservice]=WsMyService`
- `EsdlBundle/Binding/Definition[@id]=WsMyService.1`

> Note: The `@id` attribute value `WsMyService.1` has the ".1" appended to indicate the *version* of the definition. In *legacy mode* deployments the dali stores different versions of the interface distinguished with these sequential integers. *Application mode* only ever has one version so this convention is used.

### Legacy Mode

*Legacy mode* ESP deployments are best suited to running on bare metal. Part of what makes them less suitable for cloud deployments is that they require a combination of file-based and dali-based configurations. It is not desirable to require a dali server just to hold some ESP configuration.

Configuring a *legacy mode* ESP starts by using the configmgr application to customize an `environment.xml` file. An `environment.xml` file holds all the *process configuration* settings. Details about using configmgr can be found in the HPCCSystems documents *Using Configuration Manager* and *Installing and Running the HPCC Environment* found on the [documentation page](https://hpccsystems.com/training/documentation/all).

Opening the stock `environment.xml` from a platform install into configmgr gives us a good starting point for creating a *process configuration* to host a DESDL service. Starting with stock config take these steps to make it match the *process configuration* we created above for an *esdl application*:

- Add a new **Component**, of type 'esp', giving it the name MyEspInstance
- Add a new **Esp Service** of type 'DynamicESDL', assigning **Dynamic ESDL Service Name** = WsMyService
- Add a new secmgr plugin **Component** and configure. For this example we're calling it 'mysecmgr'
- Configure the MyEspInstance esp component:
  - In **ESP Service Bindings** tab, Add a binding and set these attributes:
    - **port** = 0
    - **service** = MyWsService
    - **protocol** = https
    - **securityMgrPlugin** = mysecmgr
    - Add **URL Authentication** entry with these values:
      - **description** = 'Root access to service'
      - **path** = /
      - **resource** = EnableSOAPInterface
      - **access** = Read
    - Add **Feature Authentication** entry with these values:
      - **authenticate** = Yes
      - **path** = AllowMyServiceExampleMethod
      - **resource** = AllowMyServiceExampleMethod
      - **description** = 'Controls access to the ExampleMethod of WsMyService'
    - Add **Secure User Settings** entry with these values:
      - **path** = PremiumUserLevel
      - **resource** = 'Premium User Level'
      - **description** = 'Value 1-3 indicating allowed level of premium features'
  - **Authentication** tab corresponds to the yaml file `authNZ` element
  - **AuthDomain** tab corresponds to the yaml file `authDomain` element
  - **HTTPS** tab corresponds to the yaml file `tls_config` element

This updates the `environment.xml` file. On startup an ESP *process configuration* named `esp.xml` is generated at `/var/lib/HPCCSystems/MyEspInstance/esp.xml` from the environment file. The *process configuration* outline looks something like this:

```xml
<Environment>
  <Software>
    <EspProcess daliServers=".:7070" description="FSMA ESP configured like FSMA for auth and logging" name="MyEspInstance">
      <AuthDomains>...</AuthDomains>
      <SecurityManagers>
        <SecurityManager name="mysecmgr" instanceFactoryName="" libName="" type="MyPluginSecmgrType">
          ...
        </SecurityManager>
      </SecurityManagers>
      <EspProtocol name="https" type="secure_http_protocol" plugin="esphttp">
        <certificate>...</certificate>
        <privatekey>...</privatekey>
        ...
      </EspProtocol>
      <EspService name="WsMyService"> ... </EspService>
      <EspBinding service="WsMyService" protocol="https" type="EsdlBinding" netAddress="0.0.0.0" port="0">
        <Authenticate method="mysecmgr" >
          <Location path="/" resource="EnableSOAPInterface" required="Read" description="Root access to service"/>
          <Feature path="AllowMyServiceExampleMethod" resource="AllowMyServiceExampleMethod" required="" description="Controls access to the ExampleMethod of WsMyService"/>
          <Setting path="PremiumUserLevel" resource="Premium User Level" description="Value 1-3 indicating allowed level of premium features"/>
          ...
        </Authenticate>
      </EspBinding>
      <Authentication method="secmgrPlugin"/>
    </EspProcess>
    ...
  </Software>
</Environment>
```

This deployment style requires a running dali server for the ESP process to load its *service bundle configurations* and ESDL definitions from. Refer to the *Dynamic ESDL* document on the HPCCSytems [documentation page](https://hpccsystems.com/training/documentation/all) for details on how to load the ESDL definition and the *service bundle configuration* to the dali. The *service bundle configuration* used here is the one outlined in the section [Service Bundle Configuration Commonalities](#service-bundle-configuration-commonalities).

### Debugging

The `Method/@traceLevel` attribute controls how much pre- and post- entry point debugging information is sent to the trace file. Valid values are 1-10 with 10 being the most verbose and the default being 1. Debugging information is logged for the verbose setting `Method/@traceLevel=10`:

1. Before each entry point three elements are printed:
    1. `ORIGINAL content:`, followed by the ESDL Script *source* context before executing any of this entry point's scripts
    2. `BINDING CONFIG:` followed by the *process configuration*
    3. `TARGET CONFIG:` followed by the *service bundle configuration* section for the Method that is being executed
2. After exiting each entry point the ESP prints out the contents of the ESDL Script context with the label `Entire script context after transforms:`

Adding `@trace="..."` to any entry-point tag will include that text in error-level trace output related to that script.

### Logging

TODO: Add details about configuring logging.

### Service Security

Keeping your DESDL service secure and ensuring proper access to product features is an important part of the process of writing and configuring your service. Basic security settings can be configured with a combination of attributes in your ESDL definition file and settings in your *process configuration*. Please refer to the separate document [README-SECURITY.md](https://github.com/hpcc-systems/HPCC-Platform/tree/master/esp/esdllib/README-SECURITY.md) for details.

ESDL integration scripts can assist with enforcing more complex authorization requirments. HPCC Systems platform security model has the concept of *Secure User Properties* - properties associated with a *Secure User* authenticated with an ESP. Each Security Manager plugin defines what is included in these properties, and the ESDL Scripting engine makes them available as [system parameters](#system-parameters).

Some of the Secure User Properties can be *Features* as described in the README-SECURITY.md file. These features can be inspected and used by a script to enforce access using these XPath functions:

- [getFeatureSecAccessFlags](#getfeaturesecaccessflags)
- [secureAccessFlags](#secureAccessFlags)
- [validateFeaturesAccess](#validateFeaturesAccess)

In addition, any of the other system parameters such as `$espUserStatus` may be used by a script to help enforce authentication or authorization.

# Reference

This reference section covers details about the ESDL Scripting conventions, script entry points, script operations and supported XPath functions.

## Conventions

### Implicit Variables

These special-case variables are implicitly defined by the scripting engine. They give scripts access to characteristics of the current transaction and context, and can be used without any prior declaration. The implicit variables are:

- `method`
- `request`
- `service`
- `query`

For an example we are using a simple service with one method that has a roxie backend. It is named `WsPersonInfo` with a method named `GetPersonInfo` that has an interface defined as:

```

  ESPrequest GetPersonInfoRequest
  {
    string FirstName;
    string LastName;
  };

  ESPresponse GetPersonInfoResponse
  {
    string ResponseCode;
  };

  ESPservice [version("1.0")] WsPersonInfo
  {
    ESPmethod GetPersonInfo(GetPersonInfoRequest, GetPersonInfoResponse);
  };
```

And it is bound to the backend with this configuration:

```xml
    <Method name="GetPersonInfo"  url="https://backend.roxie.com:1234" mode="ESDL" queryname="PersonInfo_Services.GetPersonInfo" status="available"/>
```

These are the implicit variables defined by the system:

* `$method`
: The name of the ESP method invoked by the current transaction request. This is derived from the `ESPmethod` name in the ESDL definition. In our example this is the string *GetPersonInfo*.

* `$request`
: The name of the ESP request structure at the root of the current transaction request. This is derived from the `ESPrequest` name in the ESDL definition. In our example this is the string *GetPersonInfoRequest*.

* `$service`
: The name of the service containing the current transaction's method. This is derived from the `ESPservice` name in the ESDL definition. In our example this is the string *WsPersonInfo*.

* `$query`
: The name of the backend roxie query. This is pulled from the method configuration attribute `Method/@queryname`. In our example this is the string *PersonInfo_Services.GetPersonInfo*. Only methods configured with a `@queryname` will have this variable defined with a non-empty value.

### System Parameters

Each of these parameters gives the script access to some information about the current execution context. In order to reference a parameter, it must be declared in the script like so:

```xml
<es:param name="clientversion" select="'unknown'"/>
```

If the parameter is undefined, then it is assigned the value of the `@select` XPath expression. These are the available system parameters:

* `$clientversion` : The client interface version specified in the incoming request.
* `$espUserName` : The username of the user authenticated to run the current transaction.
* `$espUserPeer` : The IP address of host originating the current transaction.
* `$espUserRealm` : The 'Company' or parent entity to the currently authenticated user.
* `$espUserStatus` : Integer representing the status of the currently authenticated user (eg *Active*, *Suspended*, etc., as defined by the security plugin and backend in use)
* `$espUserStatusString` : String representing the status of the currently authenticated user (eg *Active*, *Suspended*, etc., as defined by the security plugin and backend in use)
* `$espTransactionID` : The transaction ID for the current in-flight transaction.

These are the possible User Status values:

| User Status String | User Status Integer|
| - | - |
|SecUserStatus_Inhouse | 0 |
|SecUserStatus_Active | 1 |
|SecUserStatus_Exempt | 2 |
|SecUserStatus_FreeTrial| 3 |
|SecUserStatus_csdemo | 4 |
|SecUserStatus_Rollover | 5 |
|SecUserStatus_Suspended|  6 |
|SecUserStatus_Terminated | 7 |
|SecUserStatus_TrialExpired | 8 |
|SecUserStatus_Status_Hold | 9 |
|SecUserStatus_Unknown | 10 |

In addition to the standard parameters above, the script engine creates parameters out of any *Secure User Properties* configured as part of the security manager plugin you're using. Each security manager may have a different behavior regarding what settings are considered *Secure User Properties*, so consult the documentation for the manager in question for that list. Scripts must include a parameter declaration for each *Property* they want available for their use.

## Entry Points

Generally, each entry point has default source and target nodes, which are the root nodes for read and write operations respectively. These defaults can be overridden with `@source` and `@target` attributes in the entry point, which we'll discuss here, and also using [source](#source) and [target](#target) [operations](#operations).

The `@source` and `@target` attributes in the entry point elements have some special treatment not seen when they appear in operations:

- `@source`
  - XPath can include any [Implicit Variables](#implicit-variables).
- `@target`
  - XPath can include any [Implicit Variables](#implicit-variables).
  - XPath must exist at runtime or an exception will be thrown. [A future release](https://track.hpccsystems.com/browse/HPCC-25844) will ensure the path exists without throwing an exception.

### BackendRequest
```xml
    <BackendRequest
        name="String value"
        source="XPath node expression"
        target="XPath node expression"
    />
```

Manipulate the ESDL request prior to submission to a backend processor. The input is the default ESDL request, which is the initial ESDL request with method context and gateway information added. The output is the final ESDL request, which may or may not differ from the default.

This is the opportunity to validate and/or modify the request before processing. Work performed in this script may include, among other things:

- Conditional feature authorization. Does this user have permission to submit this specifc request based on request content?
- Value normalization. Correct input variances that could adversely affect the backend processor.
- Data lookup. Fetch user information not defined by the securiy manager from a remote service or database.
- Data updates. Add user information not supplied in the original request for the backend processor's use.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String identifier used in trace output. Use is optional, but strongly encouraged. |
| @source | 0..1 | XPath expression that evaluates to an input document node. This is a shortcut to avoid a child `source` operation. Default context node is `esdl_request`. |
| @target | 0..1 | XPath expression that evaluates to an output document node. This is a shortcut to avoid a child `target` operation. Default XPath node is `final_request/$query/$request` when `$query` is populated. Otherwise the default XPath node is `final_request/$method/$request`. |

Is not applied to service methods with `@querytype` values:
- java
- cpp
- script

### BackendResponse
```xml
    <BackendResponse
        name="String value"
        source="XPath node expression"
        target="XPath node expression"
    />
```

Manipulate the backend response before giving it to the ESDL transformer. The input is the original backend response. The output is the final backend response, which may or may not differ from the original.

This is the opportunity to manipulate all response data without regard for request versioning and optionals. After completing these entry point scripts, the ESDL transformer will enforce response compliance to the WSDL.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String identifier used in trace output. Use is optional, but strongly encouraged. |
| @source | 0..1 | XPath expression that evaluates to an input document node. This is a shortcut to avoid a child `source` operation. Default context node is `initial_response`. |
| @target | 0..1 | XPath expression that evaluates to an output document node. This is a shortcut to avoid a child `target` operation. Default context node is `pre_esdl_response`.|

Is not applied to service methods with `@querytype` values:
- java
- cpp

### EsdlResponse
```xml
    <EsdlResponse
        name="String value"
        source="XPath node expression"
        target="XPath node expression"
    />
```
Manipulate the ESP response before returning it to the user. The input is the original ESP response, which is the output of the ESDL transformer. The output is the final ESP response, which may or may not differ from the original.

This is the opportunity to manipulate the user response based on what is actually present in the user response. It should only be used when a manipulation requires the original ESP response to complete. If present, the response will be processed by the ESDL transformer twice - once to construct the initial response and again to ensure the final response conforms to the WSDL.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String identifier used in trace output. Use is optional, but strongly encouraged. |
| @source | 0..1 | XPath expression that evaluates to an input document node. This is a shortcut to avoid a child `source` operation. Default context node is `initial_esdl_response`.|
| @target | 0..1 | XPath expression that evaluates to an output document node. This is a shortcut to avoid a child `target` operation. Default context node is `modified_esdl_response`.|

Is not applied to service methods with `@querytype` values:
- java
- cpp

### PreLogging
```xml
    <PreLogging
        name="String value"
        source="XPath node expression"
        target="XPath node expression"
    />
```

Manipulate the complete script context after a response is returned to the user. Changes made to the document at this point will be reflected in the data made available for logging. The inputs are the non-result datasets returned by the backend, available at the context location `logdata/LogDatasets`. Anything written into the `logdata` section will be part of the payload passed on the LogManager.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String identifier used in trace output. Use is optional, but strongly encouraged. |
| @source | 0..1 | XPath expression that evaluates to an input document node. This is a shortcut to avoid a child `source` operation. Default context node is `logdata`|
| @target | 0..1 | XPath expression that evaluates to an output document node. This is a shortcut to avoid a child `target` operation. Default context node is `logdata`. |

Is not applied to service methods with `@querytype` values:
- java
- cpp

### Service
```xml
    <Service
        name="String value"
        source="XPath node expression"
        target="XPath node expression"
    />
```

Perform the service method request action, replacing the call to a backend server. This entry point is invoked for methods with a `@querytype` value of `script`. The input is the initial ESDL request, i.e., the output of the ESDL transformer operating on the ESP request. The output document begins as an empty ESP response.

When using this entry point the BackendRequest and BackendResponse entry points aren't executed. All other entry points are processed when present and all normal ESDL transformer runs are applied.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String identifier used in trace output. Use is optional, but strongly encouraged. |
| @source | 0..1 | XPath expression that evaluates to an input document node. This is a shortcut to avoid a child `source` operation. Default context node is `script_request`.|
| @target | 0..1 | XPath expression that evaluates to an output document node. This is a shortcut to avoid a child `target` operation. Default context node is `script_response`.|

Is not applied to service methods with `@querytype` values:
- java
- cpp
- roxie
- wsecl
- proxy

## XPath

ESDL Integration Scripting supports XPath 1.0. Any attributes that accept XPaths may use the functions described in this section unless otherwise noted. Refer to the [XPath 1.0 spec](https://www.w3.org/TR/1999/REC-xpath-19991116/) for the definitive explanation of available features.

### Variable Resolution

Variables in XPath are referenced by prepending a `$` to the variable's name. Any variables or parameters defined in an ESDL Script can be used in XPaths in that script.

With the exceptions noted in the [Extensions](#extensions) section, variables can be used only as allowed in the XPath spec. In the terms used in the spec a variable can't be used as an *Axis* to identify a node in a *Location Path*. A variable can only appear in an *Expression*, function *Argument* or in a *Predicate* section of a *Location Path*. Examples below will illustrate these different cases.

This sample XML will be our source context for the examples:

```xml
  <foo>
    <bar>
      <do name="deer">a deer</do>
      <do name="doe">a female deer</do>
      <ray>sunlight</ray>
      <mi>a name</mi>
      <mi>call myself</mi>
    </bar>
  </foo>
```

And say we have these ESDL Script variables defined:

```xml
 <es:variable name="barVar" value="'bar'"/>
 <es:variable name="twoVar" value="2"/>
 <es:variable name="deerVar" value="'deer'"/>
```

#### Location Paths and Predicates

This XPath shows incorrect use of a variable as an *Axis* of a *Location Path* to select the node `ray`.

```xml
  <es:set-value target="result" select="foo/$barVar/ray"/>
```

This XPath fails to properly resolve.

To achieve the same thing, we can use a *Predicate* to select any node whose local name (the name without using namespaces) is `bar`:

```xml
  <es:set-value target="result" select="foo/*[local-name()=$barVar]/ray">
```

Yielding:

```xml
  <result>sunlight</result>
```

Similarly, we can use a predicate to select the second occurrence of the `mi` node:

```xml
  <es:set-value target="result" select="foo/bar/mi[$twoVar]"/>
```

Yielding:

```xml
  <result>call myself</result>
```

Or we could use a predicate to select the `do` node with a `name` attribute equal to *deer*:

```xml
  <es:set-value target="result" select="foo/bar/do[@name=$deerVar]"/>
```

Yielding:

```xml
  <result>a female deer</result>
```

#### Expressions and Function Arguments

Variables can also be referenced in *Expressions* and in function *Arguments*. At its simplest an *Expression* can be a variable reference. This example shows setting the `result` node to the value of a `deerVar`:

```xml
  <es:set-value target="result" select="$deerVar"/>
```

Yields this:

```xml
  <result>deer</result>
```

This more complex expression with a variable:

```xml
  <es:set-value target="result" select="$twoVar + 10"/>
```

Yields:

```xml
  <result>12</result>
```

Variables can also be referenced in *Arguments* to functions:

```xml
  <es:set-value target="result" select="string-length($deerVar) + 10"/>
```

Yielding:

```xml
  <result>14</result>
```

### Extensions

ESDL Scripts extend standard XPath features to make setting entry point default `@source` and `@target` nodes easier. In any entry point element, the `@source` and `@target` XPaths may contain the [Implicit Variables](#implicit-variables) as *Axes*. These special variable references are wrapped in curly braces.

For example, say we have the following simplified request bound for a roxie backend that we're going to manipulate with a `BackendRequest` entry point script:

```xml
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <MyMethodModule.MyMethodQuery>
      <Context>...</Context>
      <MyMethodRequest>
        <Row>
          <Options></Options>
        </Row>
      </MyMethodRequest>
    </MyMethodModule.MyMethodQuery>
  </soap:Body>
</soap:Envelope>
```

The goal is to have a single script for a service that adds several nodes to the `Options` section for every method in the service. Each method has a different roxie query name (here `MyMethodModule.MyMethodQuery`) and different method request name (here `MyMethodRequest`). This script will correctly set the default target for all methods:

```xml

<es:BackendRequest name="SetServiceOptions"
                   target="soap:Body/{$query}/{$request}/Row/Options"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:es="urn:hpcc:esdl:script">
  <es:set-value target="AccessLevel" select="1"/>
  <es:set-value target="FeatureSet" select="'all'"/>
</es:BackendRequest>
```

Resulting in this request sent to the roxie:

```xml
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <MyMethodModule.MyMethodQuery>
      <Context>...</Context>
      <MyMethodRequest>
        <Row>
          <Options>
            <AccessLevel>1</AccessLevel>
            <FeatureSet>all</FeatureSet>
          </Options>
        </Row>
      </MyMethodRequest>
    </MyMethodModule.MyMethodQuery>
  </soap:Body>
</soap:Envelope>
```

### Namespaces

The scripting engine requires correct use of namespaces. A frequent source of scripting errors is omitting namespaces where they're required or using an incorrect namespace or namespace prefix. Namespaces must match between the context XML document and script XPaths referencing the context XML document. It is not required for the prefix in the script's XPath to match the prefix in the document.

In most cases this means that a script needs to define matching namespace URIs. There are two exceptions:

1. The engine will automatically detect and define the standard soap namespace ( `xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"` ) with the prefix `soap` if you use an XPath that incudes `Body` or `Envelope` nodes.
2. The prefix `n` is defined to refer to the current default namespace (a namespace with no prefix), and it can be used in script XPaths if desired.

For the examples below, say we have this source XML:

```xml
<esdl_request>
    <request url="http://foo.bar.com:9876">
      <content>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <country>us</country>
            <special_services.my_service xmlns="urn:hpccsystems:ecl:special_services.my_service">
              <city>minneapolis</city>
              <state>mn</state>
              <zip>55419</zip>
              <nested xmlns="urn:hpccsystems:ecl:special_services.my_service:nested">
                <phone>6125551212</phone>
              </nested>
            </special_services.my_service>
          </soap:Body>
        </soap:Envelope>
      </content>
    </request>
</esdl_request>
```

#### soap Example
Say we are processing a BackendRequest script. Knowing the special handling of the soap namespace we could write a script as below to refer to the `country` node:

```xml
<BackendRequest name="TestScript" xmlns:es="urn:hpcc:esdl:script">
    <es:variable name="country-value" select="request/content/soap:Envelope/soap:Body/country"/>
</BackendRequest>
```

#### Default Namespace Examples

To refer to the `city` node in the example using the special default prefix `n`, the script would look like this:

```xml
<BackendRequest name="TestScript"
                xmlns:es="urn:hpcc:esdl:script"
                xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <es:variable name ="city-value"
                 select="request/content/soap:Envelope/soap:Body/n:special_services.my_service/n:city"/>
</BackendRequest>
```

Alternately you can define the namespace with a prefix and use that prefix in your XPath:

```xml
<BackendRequest name="TestScript"
                xmlns:es="urn:hpcc:esdl:script"
                xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:insvc="urn:hpccsystems:ecl:special_services.my_service">
    <es:variable name ="city-value"
                 select="request/content/soap:Envelope/soap:Body/insvc:special_services.my_service/insvc:city"/>
</BackendRequest>
```

The special default prefix `n` is only valid for the first default namespace encountered in an XPath. To refer to the `phone` node above, you could use an XPath like this:

```xml
<BackendRequest name="TestScript"
                xmlns:es="urn:hpcc:esdl:script"
                xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:spnest="urn:hpccsystems:ecl:special_services.my_service:nested">
    <es:variable name ="city-value"
                 select="request/content/soap:Envelope/soap:Body/n:special_services.my_service/spnest:nested/spnest:phone"/>
</BackendRequest>
```

### EXSLT Functions

In addition to the spec XPath functions, we support a subset of EXSLT-Functions (Extended XSLT) for use in XPath expressions. The exslt functions are grouped into modules of related features like *strings*, *sets* and *math*. There are different implmentations of the modules and not all implementations support all functions. We are using the `libxslt` implementation. The reference at [exslt.org](http://exslt.org/func/index.html) lists the functions in each module and which implementations support each function. See below for details of our supported modules

| Module Name | Namespace Prefix | libxslt Supported Functions |
|-------------|------------------|-----------------------------|
|[dates and times](http://exslt.org/date/index.html)| date | all except `date:format-date`, `date:parse-date`, `date:sum`, `date:date-format`
|[math](http://exslt.org/math/index.html)| math | all
|[sets](http://exslt.org/set/index.html)| set | all except `set:distinct`
|[strings](http://exslt.org/str/index.html) | str | all except `str:decode-uri`, `str:encode-uri`, `str:replace`, `str:split`

### Custom Functions

ESDL Integration Scripting defines several custom XPath functions described below.

#### ensureDataSection
    node-set ensureDataSection(section_name)

Ensures the script context section named *section_name* exists and returns it as a node-set. Returns an empty node set if the XPath evaluation of *section_name* fails.

| Parameter | Required? | Description
| - | - | -
|section_name | no | XPath expression evaluating to the name of the context section to act on. If omitted, defaults to `temporaries`.

##### Example

Start with the script context in this state:

```xml
<esdl_script_context>
    <esdl_request>...</esdl_request>
</esdl_script_context>
```

Execute this entry point script:

```xml
<es:BackendRequest xmlns:es="urn:hpcc:esdl:script">
    <es:set-value target="ensureDataSection('mySection')/firstValue" value="23"/>
</es:BackendRequest>
```
Result is this script context:

```xml
<esdl_script_context>
    <esdl_request>...</esdl_request>
    <mySection>
        <firstValue>23</firstValue>
    </mySection>
    <final_request>...<final_request>
</esdl_script_context>
```

#### getDataSection
    node-set getDataSection(section_name)

Returns the node-set of the xml node named *section_name* of the script context. Returns an empty node-set if the XPath evaluation of *section_name* fails.

| Parameter | Required? | Description
| - | - | -
|section_name | no | XPath expression evaluating to the name of the context section to retrieve. If omitted, defaults to `temporaries`.

##### Example

Assume we have this script context

```xml
<esdl_script_context>
    <esdl_request>...</esdl_request>
    <mySection>
        <firstValue>23</firstValue>
    </mySection>
    <final_request>...<final_request>
</esdl_script_context>
```

then execute this entry point script:

```xml
<es:BackendRequest xmlns:es="urn:hpcc:esdl:script">
    <es:set-value target="MyFirstValue" select="getDataSection('mySection')/firstValue"/>
</es:BackendRequest>
```
The resulting script context is:

```xml
<esdl_script_context>
    <esdl_request>...</esdl_request>
    <mySection>
        <firstValue>23</firstValue>
    </mySection>
    <final_request>
        ...
        <MyFirstValue>23</MyFirstValue>
    <final_request>
</esdl_script_context>
```

#### getFeatureSecAccessFlags
    number getFeatureSecAccessFlags(feature_name)

Returns the internal representation of the access granted to the authenticated ESP user for the indicated feature resource.

| Parameter | Required? | Description
| - | - | -
|feature_name | no | XPath expression evaluating to the name of a defined feature resource.

These are the numeric values returned associated with the different access levels:

| Number | Access Level Name |
|--------|-------------------|
| -1     | Unavailable       |
| -255   | Unknown           |
|  0     | None              |
|  1     | Access            |
|  3     | Read              |
|  7     | Write             |
|  255   | Full              |

#### getLogOption
    string getLogOption(option)

Determines if a named log option has been set. If set, the option value is returned. If not set, returns an empty string.

In the context of log agents and their variant identifiers, a log option generally corresponds to a variant type identifier. It is acceptable to set options that are not agent type identifiers.

| Parameter | Required? | Description
| - | - | -
|feature_name | no | XPath expression evaluating to the name of a log option.

#### getLogProfile
    string getLogProfile()

Determines if a log profile has been set. If set, the profile name is returned. If not set, returns an empty string.

In the context of log agents and their variant identifiers, the log profile corresponds to a variant group identifier.

#### getStoredStringValue
    string getStoredStringValue(value_name)

Determines if a named value has been added to the script context's `store` section. Returns the value if it exists. Returns an empty string if it does not exist.

| Parameter | Required? | Description
| - | - | -
|value_name | yes | XPath expression evaluating to the name of a stored string value.

#### logOptionExists
    boolean logOptionExists(option_name)

Determines if a named log option has been set. Returns *true* if set. Returns *false* if not set.

In the context of log agents and their variant identifiers, a log option generally corresponds to a variant type identifier. It is acceptable to set options that are not agent type identifiers.

| Parameter | Required? | Description
| - | - | -
|option_name | yes | XPath expression evaluating to the name of a log option.

#### secureAccessFlags
    number secureAccessFlags(access-level-1, ..., access-level-n)

Returns a bit-mask containing the internal representations of each given access level.

| Parameter | Required? | Description
| - | - | -
| access-level-1 | yes | The text representation of a defined resource access level. One value is required. |
| access-level-2 ... access-level-n | no | The text representation of a defined resource access level. |

The defined access level strings are:

* Unavailable
* None
* Access
* Read
* Write
* Full

#### storedValueExists
    boolean storedValueExists(value_name)

Determines if a named value exists in the script context's `store` section. Returns *true* if the name exists. Returns *false* if the name does not exist.

| Parameter | Required? | Description
| - | - | -
|value_name | yes | XPath expression evaluating to the name of a stored string value.

#### tokenize
    node-set tokenize(string)
    node-set tokenize(string, delimiters)

Breaks an input text string into tokens based on delimiters. The tokens are temporarily added to the script context as individual nodes which can be used by script operations that act on node-sets.

| Parameter | Required? | Description
| - | - | -
|string | yes | Text string possibly containing delimited text to be split into tokens.
| delimiters | no | Optional set of characters used to split *string* into tokens. Defaults to tab, carriage return, line feed, and space, if omitted. |

#### Example

This script:

```xml
  <es:element name="NewNode">
    <es:copy-of select="tokenize('a,b,c,d', ',')"/>
  </es:element>
```

Will give this result in the target:

```xml
  <NewNode>
    <token>a</token>
    <token>b</token>
    <token>c</token>
    <token>d</token>
  <NewNode>
```

#### validateFeaturesAccess
    boolean validateFeatureAccess(access_list)

Returns *true* if the authenticated ESP user has been granted the minimum required access for each requested feature. Returns *false* if the authenticated ESP user has not been granted the minimum required access for at least one requested feature.

| Parameter | Required? | Description
| - | - | -
|access_list | yes | A comma-delimited string of access tokens. Each access tokens takes the form of `feature-name [ ':'  access-level ]`, where `feature-name` is the name of a defined feature resource and `access-level` is the required permission level. If `access-level` is omitted, *READ* access is required.

## Operations

### add-value
```xml
    <add-value
        optional="Boolean value"
        name="string value"
        required="Boolean value"
        select="XPath evaluated as string"
        target="string value"
        trace="string value"
        value="string value"
        xpath_target="XPath evaluated as string"
    />
```

Given an output destination XPath, add a new occurrence of the path's leaf element, building the path's elements along the way. The path to the leaf must be uniquely identified, but the leaf itself may be repeated any number of times.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @name | 0..1 | Alternate value for trace output when `@trace` is empty. This exists for backward compatibility. |
| @required | 0..1 | Boolean flag indicating whether a failure to traverse the path to the leaf yields an exception (*true*) or silent failure (*false*). |
| @select | 0..1 | XPath expression evaluated to produce the new element's value. |
| @target | 0..1 | XPath for the element to be created, if `@xpath_target` is omitted. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @value | 0..1 | The new element's value, if `@select` is omitted. |
| @xpath_target | 0..1 | XPath expression evaluated to an XPath identifying the element to be added. |

- While `target` and `xpath_target` are individually optional, at least one must be given. If both are given, `xpath_target` is used.
- While `select` and `value` are individually optional, at least one must be given. If both are given, `select` is used.

### append-to-value
```xml
    <append-to-value
        optional="Boolean value"
        name="string value"
        required="Boolean value"
        select="XPath evaluated as string"
        target="string value"
        trace="string value"
        value="string value"
        xpath_target="XPath evaluated as string"
    />
```

Given an output destination XPath, add a new element with the given value, if one does not already exist, or append the given value to the existing element value. The path must uniquely identify an element to be created or updated.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @name | 0..1 | Alternate value for trace output when `@trace` is empty. This exists for backward compatibility. |
| @required | 0..1 | Boolean flag indicating whether a failure to traverse the path to the leaf yields an exception (*true*) or silent failure (*false*). |
| @select | 0..1 | XPath expression evaluated to produce the new value content. |
| @target | 0..1 | XPath for the element to be updated or created, if `@xpath_target` is omitted. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @value | 0..1 | The element content update, if `@select` is omitted. |
| @xpath_target | 0..1 | XPath expression evaluated to an XPath identifying the element to be updated or created. |

- While `target` and `xpath_target` are individually optional, at least one must be given. If both are given, `xpath_target` is used.
- While `select` and `value` are individually optional, at least one must be given. If both are given, `select` is used.

_For backward compatibility, `AppendValue` is an accepted synonym for this operation, though we discourgae its use._

### assert
```xml
    <assert
        code="XPath integer expression"
        message="XPath text expression"
        name="text value"
        optional="Boolean value"
        test="XPath Boolean expression"
        trace="string"
    />
```

Terminate transaction processing with an error code and message when `@test` evaluates to *false*.

> **Caution**: Available to any *entry point*, the operation's purpose is to short circuit transaction processing before submitting a request to the backend processor. It is likely inappropriate to short circuit processing once the backend processor has produced a result or logging is required.

| Property | Count | Description |
| - | - | - |
| @code | 1..1 | XPath expression that evaluates to an integer. The integer becomes the error code in a thrown exception. |
| @message | 1..1 | XPath expression that evaluates to text. The text becomes the error message in a thrown exception. |
| @name | 0..1 | Text label used only when `@trace` is absent. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @test | 1..1 | XPath expression that evaluates to a Boolean. Throw an exception if *true*. Do nothing if *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

The operation supports no child operations.

### choose
```xml
    <choose
      optional="Boolean value"
      trace="string">
        <when
          optional="Boolean value"
          test="XPath Boolean expression"
          trace="string"
        />
        <otherwise
          optional="Boolean value"
          trace="string"
        />
    </choose>
```

Selects at most one child to be processed. Each `when/@test` condition is evaluated, in defined order. If evaluation yields *true*, that clause's child operations are processed and this operation terminates. If no evaluation yields *true* and an `otherwise` clause is present, the children of `otherwise` are processed and this operation terminates.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| when | 0..n | An operation block to be processed if its `@test` expression evaluates to *true*, and to be ignored if its `@test` expression evaluates to *false*. |
| otherwise | 0..1 | An operation block to be processed if no occurrence of `when` is processed. |

- While `when` and `otherwise` are both optional on their own, at least one of either is required. Both are operation blocks capable of containing operations.
- Child elements other than `when` and `otherwise` are silently ignored during load. Parsing succeeds, but runtime outcome will not be as expected.

### copy-of
```xml
    <copy-of
        new_name="String value"
        optional="Boolean value"
        select="XPath node-set value"
        trace="string"
    />
````

Creates copies of selected nodes, optionally renaming them, and adds them to the current target node.

| Property | Count | Description |
| - | - | - |
| @new_name | 0..1 | Node name assigned to copies. If empty, copies are not renamed. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @select | 1..1 | Path to nodes to be copied. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

- This operation contains no child operations.

### element
```xml
    <element
        name="String value"
        namespace="String value"
        optional="Boolean value"
        trace="String value"
    />
```

Add an element to the current target node with the given name. Any child operations use the new element as their default target.

| Property | Count | Description |
| - | - | - |
| @name | 1..1 | Valid XML element name. |
| @namespace | 0..1 | Reserved for future use. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

### ensure-target
```xml
    <ensure-target
        optional="Boolean value"
        required="Boolean value"
        trace="String value"
        xpath="XPath node expression"
    />
```

Temporarily change the script's current target to an existing or new output document node. Relative paths specified in child operations are relative to the indicated node. The effective target resets to its previous value upon completion of child processing.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *true*. |
| @required | 0..1 | Boolean flag indicating whether a failure to uniquely identify an existing output node terminates processing (*true*) or skips processing child operations (*false*). Defaults to *true*.|
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath | 1..1 | XPath expression evaluating to document node. If that node does not exist it is created. |

See also [if-target](#if-target) and [target](#target).

### fail
```xml
    <fail
        code="XPath integer expression"
        message="XPath text expression"
        name="text value"
        optional="Boolean value"
        trace="string"
    />
```

Terminate transaction processing with an error code and message.

> **Caution**: Available to any *entry point*, the operation's purpose is to short circuit transaction processing before submitting a request to the backend processor. It is likely inappropriate to short circuit processing once the backend processor has produced a result or logging is required.

| Property | Count | Description |
| - | - | - |
| @code | 1..1 | XPath expression that evaluates to an integer. The integer becomes the error code in a thrown exception. |
| @message | 1..1 | XPath expression that evaluates to text. The text becomes the error message in a thrown exception. |
| @name | 0..1 | Text label used only when `@trace` is absent. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

The operation supports no child operations.

### for-each
```xml
    <for-each
        optional="Boolean value"
        select="XPath node-set expression"
        trace="String value"
    />
```

Iterates a possibly empty set of input nodes. Child operations are processed once for each node, with the selected node as the current source node.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @select | 1..1 | XPath expression that identifying all input nodes to be iterated.
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

### http-post-xml
```xml
    <http-post-xml
        optional="Boolean value"
        trace="String value"
        url="String value"
        section="XPath node expression"
        name="String value">
        <http-header name="String value"
                     xpath_name="XPath string expression"
                     value="XPath string expression"/>
        <content>...</content>
    </http-post-xml>
```

Create then send an HTTP post message with XML content. Content type of the outgoing request is `text/xml`.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @url | 1..1 | Endpoint to send the HTTP post message. |
| @section | 0..1 | Path to the section of script context where output is placed. If omitted defaults to `temporaries`.|
| @name | 1..1 | Name of the node inside `@section` where the output is placed. If it does not exist it is created. |
| http-header | 0..n | An HTTP Header name and value to include with the POST. Use one element for each header |
| http-header/@name | 0..1 | String value giving the name of the HTTP header.|
| http-header/@xpath-name | 0..1 | XPath expression evaluated as a string giving the name of the HTTP header. |
| http-header/@value | 1..1 | The value of the HTTP header. |
| content | 1..1 | Contains child script operations that construct the XML payload of the HTTP Post. |

Note:

- While `http-header/@name` and `http-header/@xpath-name` are individually optional, at least one is required. If both are provided then `@xpath-name` is used.
- Child elements other than `http-header` and `content` are ignored.
- Multiple `http-header` child elements can be used.
- Only one `content` child element should be used. If multiple are present the last one in lexical order is used.

On completion of the HTTP transaction, the script context is updated as below:

- *`section`*`/`*`name`*`/request/content` holds the constructed request
- *`section`*`/`*`name`*`/response` holds the HTTP response
- *`section`*`/`*`name`*`/response/@status` holds the HTTP response status
- *`section`*`/`*`name`*`/response/@error-code` holds the HTTP response error code, zero indicating success
- *`section`*`/`*`name`*`/response/@content-type` holds the _Content-Type_ header value of the HTTP response

#### Example

Say we have an ESDL Script that calls out to a roxie to get a *userid* number prior to logging. In this example we're hard-coding values for the outgoing request, but in a actual script those values would likely be copied from another location of the script context.

```xml
  <es:PreLogging name="LogPrep"
                 xmlns:es="urn:hpcc:esdl:script"
                 xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                 xmlns:svc="urn:hpccsystems:ecl:misc_services.log_service"
                 xmlns:resp="urn:hpccsystems:ecl:misc_services.log_service:result:results">
    <es:http-post-xml url="'http://roxie.my.domain:9876'"
                      section="logdata/LogDatasets"
                      name="LogPrep">
      <es:content>
        <es:element name="Envelope">
          <es:namespace prefix="soap"
                        uri="http://schemas.xmlsoap.org/soap/envelope/"
                        current="true" />
          <es:element name="Body">
            <es:element name="misc_services.log_service">
              <es:namespace uri="urn:hpccsystems:ecl:misc_services.log_service"
                            current="true" />
                <es:set-value target="FirstName" value="'John'"/>
                <es:set-value target="LastName" value="'Baker'"/>
            </es:element>
          </es:element>
        </es:element>
      </es:content>
    </es:http-post-xml>

    <!--
        Pull userid from roxie response to use in subsequent processing
    -->
    <es:variable name="userid" select="getDataSection('logdata')/LogDatasets/LogPrep/response/content/soap:Envelope/soap:Body/svc:misc_services.log_serviceResponse/svc:Results/svc:Result/resp:Dataset/resp:Row/resp:userid"/>

  </es:PreLogging>
```

After executing this script, and assuming a successful POST to the roxie, the script context would look like this:

```xml
<esdl_script_context>
  <logdata>
    <LogDatasets>
      <LogPrep content-type="text/xml"
               status="OK"
               status-code="0">
        <request>
          <content>
            <soap:Envelope>
              <soap:Body>
                <svc:misc_services.log_service>
                  <svc:FirstName>John</svc:FirstName>
                  <svc:LastName>Baker</svc:LastName>
              </soap:Body>
            </soap:Envelope>
          </content>
        </request>
        <response>
          <content>
            <soap:Envelope>
              <soap:Body>
                <svc:misc_services.log_serviceResponse>
                  <svc:Results>
                    <svc:Result>
                      <resp:Dataset>
                        <resp:Row>
                          <resp:userid>1234</resp:userid>
                        </resp:Row>
                      </resp:Dataset>
                    </svc:Result>
                  </svc:Results>
                </svc:misc_services.log_serviceResponse>
              </soap:Body>
            </soap:Envelope>
          </content>
        </response>
      </LogPrep>
    <LogDatasets>
  </logdata>
</esdl_script_context>
```

This example also shows how correct namespaces must be declared in an entry point if any operations will refer to nodes using those namespaces.

### if
```xml
    <if
        optional="Boolean value"
        test="XPath Boolean expression"
        trace="string"
    />
```

Evaluates `@test` to decide whether child operations are or are not processed. An evaluated result of *true* processes children, and a result of *false* skips children.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @test | 1..1 | XPath expression to be evaluated. The result is interpreted as a Boolean. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

### if-source
    <if-source
        optional="Boolean value"
        trace="String value"
        xpath="XPath node expression"
    />

If the `@xpath` exists, temporarily change the script's current source to that node and execute any child operations. Relative paths in child operations are relative to the indicated node. The effective source resets to its previous value upon completion of child processing.

This is a shortcut to using `<source xpath="..." required="false"/>`

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath | 1..1 | XPath expression evaluating to an existing input document node. |

See also [source](#source).

### if-target
```xml
    <if-target
        optional="Boolean value"
        trace="String value"
        xpath="XPath node expression"
    />
```

If the `@xpath` exists, temporarily change the script's current target to that node and execute any child operations. Relative paths in child operations are relative to the indicated node. The effective target resets to its previous value upon completion of child processing.

This is a shortcut to using `<target xpath="..." required="false"/>`.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath | 1..1 | XPath expression evaluating to an existing output document node. |

See also [ensure-target](#ensure-target) and [target](#target).

### mysql
```xml
    <mysql
        database="String value"
        MYSQL_...="String value"
        name="String value"
        optional="Boolean value"
        password="String value"
        resultset-tag="XPath string expression"
        secret="String value"
        section="XPath node expression"
        select="XPath node expression"
        server="String value"
        trace="String value"
        user="String value"
        vault="String value">
        <bind name="String value"
              value="XPath string expression"/>
        <sql>...</sql>
    </mysql>
```

| Property | Count | Description |
| - | - | - |
| @database | 0..1 | Name of the database to run the sql statement on. Required if not provided in a _vault_ or _secret_. See the following [Credentials](#credentials) section for further explanation.|
| @MYSQL_... | 0..n | A MySQL option to set on the sql transaction. The full name of the attribute should match a MySQL option enum name e.g. `MYSQL_SET_CHARSET_NAME` |
| @name | 0..1 | Name of the node inside `@section` where the output is placed. If it does not exist it is created. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @password | 0..1 | String of the password to login to the database server. Required if not provided in a _vault_ or _secret_. See the following [Credentials](#credentials) section for further explanation.|
| @resultset-tag | 0..1 | The name of the node holding the resultset generated by executing the sql statement. This node is created in the script context at the location *`section`*`/`*`name`*`/`*`resultset-tag`*|
| @secret | 0..1 | String of the secret name to retrieve database login info from. The category name used is *esp*. For security, preferred usage is to include user, password, server and database together. See the following [Credentials](#credentials) section for further explanation. |
| @section | 0..1 | Path to the section of script context where output is placed. If omitted defaults to `temporaries`.|
| @select | 0..1 | XPath node expression used as root of the source for binding sql parameters. When provided, the mysql operation will run once for each node in the expression result. |
| @server | 1..1 | String value of the database server in the format *ip:port* or *hostname:port*. Required if not provided in a _vault_ or _secret_. See the following [Credentials](#credentials) section for further explanation.|
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @user | 0..1 | String of the user name to login to the database server. Required if not provided in a _vault_ or _secret_. See the following [Credentials](#credentials) section for further explanation. |
| @vault | 0..1 | String of the vault ID to retrieve database login info from. The category name used is *esp*. For security, preferred usage is to include user, password, server and database together. See the following [Credentials](#credentials) section for further explanation. |
| bind | 0..n | Node to bind a value to a sql parameter. |
| bind/@name | 1..1 | String of the name of the sql parameter. Use only one instance per `bind` node. |
| bind/@value | 1..1 | XPath string expression of the value of the parameter. Use only one instance per `bind` node. |
| sql | 1..1 | Required element whose contents are an sql statement to execute. |

#### Credentials

The required credentials are *server*, *user*, *password* and *database*. They can be supplied to this operation three ways: from a vault, from a local secret store or directly in attributes. Only *one* set of the following attributes are required to be populated in order to supply credentials:

| Vault    | Local Secrets | Direct Attributes                  |
|----------|---------------|------------------------------------|
| `@vault` | `@secret`     | `@server`, `@user`, `@password` and `@database` |

The system looks for values first in the vault, then the secrets store and finally in the direct attributes. The credential lookup process is:

1. If the vault exists use credentials from the vault. If any value is missing from the vault attempt to use the value from the direct attribute. If the attribute doesn't provide a value then an exception is thrown.
2. If the local secrets store exists use the credentials from the local secrets store. If any value is missing from the store attempt to use the value from the direct attribute. If the attribute doesn't provide a value then an exception is thrown.
3. If neither the vault nor the store exist then attempt to use all values from the direct attributes. If any attribute doesn't provide a value then an exception is thrown.

A discussion of how to use a vault is beyond the scope of this document, but we'll outline how to use the local secrets store.

The local secrets store is a mount point containing *category* folders. Each *category* folder further contains *secret* folders, which contains files named for one piece of secret info. Each info file contains the secret value corresponding to the file name. In a standard HPCCSystems platform installation, the secrets mount point is `/opt/HPCCSystems/secrets`, though this is determined by the location of the running esp executable. The ESDL Script secrets use the `esp` category name, so if our secret is set to `@secret="FooBarDBQA"` the files required to supply credentials for this operation are:

* `/opt/HPCCSystems/secrets/esp/FooBarDBQA/server`
* `/opt/HPCCSystems/secrets/esp/FooBarDBQA/user`
* `/opt/HPCCSystems/secrets/esp/FooBarDBQA/password`
* `/opt/HPCCSystems/secrets/esp/FooBarDBQA/database`

#### Examples

Say we have a MySQL database `foo` with table `bar` running in a QA environment on 192.168.1.1. We will connect to it with the username *testuser* and password *Password1*. It was created with this schema:

```SQL
CREATE TABLE `bar` (
  `unique_id`   int NOT NULL,
  `inserted_at` datetime NOT NULL,
  `name`        varchar(30),
  `phone`       varchar(10),
  PRIMARY KEY (unique_id, inserted_at)
);
```
Our secrets files are setup as below:

| File | Contents |
|------|----------|
| `secrets/esp/FooBarDBQA/server` | 192.168.1.1 |
| `secrets/esp/FooBarDBQA/user` | testuser |
| `secrets/esp/FooBarDBQA/password` | Password1 |
| `secrets/esp/FooBarDBQA/database` | foo |

The contents of the database are:

| unique_id | inserted_at | name | phone |
|-----------|-------------|------|-------|
| 100       | 2022-01-01 02:10:03 | Robin Anderson | 3205551212 |
| 101       | 2022-01-01 03:13:27 | Chris Bakerson | 9525551212 |
| 102       | 2022-03-24 14:30:06 | Mia Smith | 9175551212 |
| 103       | 2022-04-16 20:43:18 | Italo Calvino | 2125551212  |
| 104       | 2022-05-02 07:16:57 | Octavia Yamoto | 3075551212 |

Then we execute these `mysql` operations:

```xml
  <es:variable name="inputs" select="tokenize('100 102 104')"/>
  <es:mysql name="SQL1"
            secret="'FooBarDBQA'"
            section="'foobar'">
    <es:sql>
      select count(*) as count from foo.bar where inserted_at &gt;= '2022-03-24';
    </es:sql>
  </es:mysql>
  <es:mysql name="SQL2"
            secret="'FooBarDBQA'"
            section="'foobar'"
            resultset-tag="'OneZeroThree'">
    <es:bind name="unique_id" value="103"/>
    <es:sql>
      select * from foo.bar where unique_id = ?;
    </es:sql>
  </es:mysql>
  <es:mysql name="SQL2"
            select="$inputs/token"
            secret="'FooBarDBQA'"
            section="'foobar'"
            resultset-tag="'EvenPhones'">
    <es:bind name="unique_id" value="."/>
    <es:sql>
      select unique_id, phone from foo.bar where unique_id = ?;
    </es:sql>
  </es:mysql>
```

Giving this result:

```xml
  <foobar>
    <SQL1>
      <Row>
        <count>3</count>
      </Row>
    </SQL1>
    <SQL2>
      <OneZeroThree>
        <Row>
          <unique_id>103</unique_id>
          <inserted_at>2022-04-16 20:43:18</inserted_at>
          <name>Italo Calvino</name>
          <phone>2125551212</phone>
        </Row>
      </OneZeroThree>
      <EvenPhones>
        <Row>
          <unique_id>100</unique_id>
          <phone>3205551212</phone>
        </Row>
        <Row>
          <unique_id>102</unique_id>
          <phone>9175551212</phone>
        </Row>
        <Row>
          <unique_id>104</unique_id>
          <phone>3075551212</phone>
        </Row>
      </EvenPhones>
    </SQL2>
  </foobar>
```

### namespace
```xml
    <namespace
        current="Boolen value"
        optional="Boolean value"
        prefix="String value"
        trace="String value"
        uri="String value"
    />
```
Create or use an XML namespace in the output document.

| Property | Count | Description |
| - | - | - |
| @current | 0..1 | Boolean flag indicating whether the identified namespace applies to the output document's current node (*true*) or does not (*false*). Defaults to *true* when `@uri` is empty and *false* otherwise. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @prefix | 0..1 | Valid XML namespace prefix string or omitted; must not be empty. Omit to define the default namespace (i.e., the namespace with no prefix). Specify an existing namespace prefix with no `@uri` to refer to the existing namespace. Specify a new prefix with an `@uri` value to define a namespace. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @uri | 0..1 | Namespace value to be defined. Omit to refer to an existing namespace. |

- While `@prefix` and `@uri` are both individually optional, at least one must be given.

### param
```xml
    <param name="String value"
           select="XPath expression"
    />
```

Define an immutable parameter for use by the script. It is in scope for any sibling or descendent elements.

| Property | Count | Description |
| - | - | - |
| @name | 1..1 | Required name of the parameter. |
| @select | 0..1 | Evaluation of this XPath expression gives the default value of the parameter if no value is supplied by the script engine. If omitted the default value is an empty string. |

### remove-node
```xml
    <remove-node
        all="Boolean value"
        optional="Boolean value"
        target="String value"
        trace="String value"
        xpath_target="XPath node-set value"
    />
```

Remove the indicated node or nodes from the output document. If `@all` is *false*, the request applies to one uniquely identified node and multiple matching nodes result in an *Ambiguous XPath* runtime error returned. If `@all` is *true*, the request applies to any number of identified nodes.

| Property | Count | Description |
| - | - | - |
| @all | 0..1 | Boolean flag indicating whether the request applies to all matching nodes (*true*) or if the request must uniquely identify a single node (*false*). Defaults to *false*. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @target | 0..1 | String referring to zero or more output document nodes. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath_target | 0..1 | XPath identifying a possibly empty set of output document nodes. |

- While `@target` and `@xpath_target` are both individually optional, at least one must be given. If both are given, `@xpath_target` is used.

### rename-node
```xml
    <rename-node
        all="Boolean value"
        new_name="String value"
        optional="Boolean value"
        target="String value"
        trace="String value"
        xpath_new_name="XPath String value"
        xpath_target="XPath node set value"
    />
```

Rename the indicated node or nodes in the output document. If `@all` is *false*, the request applies to one uniquely identified node and multiple matching nodes result in an *Ambiguous XPath* runtime error returned. If `@all` is *true*, the request applies to any number of identified nodes.

| Property | Count | Description |
| - | - | - |
| @all | 0..1 | Boolean flag indicating whether the request applies to all matching nodes (*true*) or if the request must uniquely identify a single node (*false*). Defaults to *false*. |
| @new_name | 0..1 | String containing a valid XML node name. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @target | 0..1 | String referring to zero or more output document nodes. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath_new_name | 0..1 | XPath evaluating to a valid XML node name. |
| @xpath_target | 0..1 | XPath identifying a possibly empty set of output document nodes. |

- While `@target` and `@xpath_target` are both individually optional, at least one must be given. If both are given, `@xpath_target` is used.
- While `@new_name` and `@xpath_new_name` are both individually optional, at least one must be given. If both are given, `@xpath_new_name` is used.

### set-log-option
```xml
    <set-log-option
        name="String value"
        optional="Boolean value"
        select="XPath String value"
        trace="String value"
        xpath_name="XPath String value"
    />
```

Add a name-value pair to the *logging* section of the script context. To avoid name collisions with `set-log-profile`, the name `profile` should be avoided. To disable a log agent, use the name `disable-log-type-<agent type value>`.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String label identifying a value. May be a log agent variant type identifier to control enabled log agents on a per-transaction basis. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @select | 1..1 | XPath evaluating to an option value. The value should be a Boolean if the name is a log agent variant type identifier and will be used to control a log agent's state. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath_name | 0..1 | Xpath evaluating to a value identifier. May be a log agent variant type identifier to control enabled log agents on a per-transaction basis. |

- While `@name` and `@xpath_name` are both individually optional, at least one must be given. If both are given, `@xpath_name` is used.

This option is passed to *LogAgents* in their request:

```xml
<UpdateLogRequest>
    <logging option-name="option-value"/>
</UpdateLogRequest>
```
and can be referenced from a loggingagent configuration with the xpath `logging[@`*`option-name`*`]`

See also [set-log-profile](#set-log-profile), [getLogOption](#getLogOption) and [logOptionExists](#getLogOption).

### set-log-profile
```xml
    <set-log-profile
        optional="Boolean value"
        select="XPath String value"
        trace="String value"
    />
```

Add or update a name-value pair in the *logging* section of the script context. The name is always `profile`. The value should match a log agent variant group identifier to enable or disble groups of related log agents.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @select | 1..1 | XPath evaluating to a log agent variant group identifier. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |

See also [getLogProfile](#getLogProfile).

### set-value
```xml
    <set-value
        optional="Boolean value"
        name="string value"
        required="Boolean value"
        select="XPath evaluated as string"
        target="string value"
        trace="string value"
        value="string value"
        xpath_target="XPath evaluated as string"
    />
```

Given an output destination XPath, add a new element with the given value, if one does not already exist, or replace the existing element value. The path must uniquely identify an element to be created or updated.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @name | 0..1 | Alternate value for trace output when `@trace` is empty. This exists for backward compatibility. |
| @required | 0..1 | Boolean flag indicating whether a failure to traverse the path to the leaf yields an exception (*true*) or silent failure (*false*). |
| @select | 0..1 | XPath expression evaluated to produce the new value content. |
| @target | 0..1 | XPath for the element to be updated or created, if `@xpath_target` is omitted. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @value | 0..1 | The element content update, if `@select` is omitted. |
| @xpath_target | 0..1 | XPath expression evaluated to an XPath identifying the element to be updated or created. |

- While `target` and `xpath_target` are individually optional, at least one must be given. If both are given, `xpath_target` is used.
- While `select` and `value` are individually optional, at least one must be given. If both are given, `select` is used.

_For backward compatibility, `SetValue` is an accepted synonym for this operation, though we discourgae its use._

### source
```xml
    <source
        optional="Boolean value"
        required="Boolean value"
        trace="String value"
        xpath="XPath node expression"
    />
```

Temporarily change the script's current source node. Relative paths specified in child operations are relative to the indicated node. The effective source resets to its previous value upon completion of child processing.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @required | 0..1 | Boolean flag indicating whether a failure to uniquely identify an existing input node terminates processing (*true*) or skips processing child operations (*false*). |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath | 1..1 | XPath expression evaluating to an existing input document node. |

See also [if-source](#if-source).

### store-value
```xml
    <store-value
        name="String value"
        optional="Boolean value"
        select="XPath String value"
        trace="String value"
        xpath_name="XPath String value"
    />
```

Add a name-value pair to the *store* section of the script context. The name must be valid as an XML attribute. The value must be encoded for storage as an attribute value.

| Property | Count | Description |
| - | - | - |
| @name | 0..1 | String label identifying a value. |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @select | 1..1 | XPath evaluating to an option value. |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath_name | 0..1 | Xpath evaluating to a value identifier. |

- While `@name` and `@xpath_name` are both individually optional, at least one must be given. If both are given, `@xpath_name` is used.

### target
```xml
    <target
        optional="Boolean value"
        required="Boolean value"
        trace="String value"
        xpath="XPath node expression"
    />
```

Temporarily change the script's target node to an existing output document node. Relative paths specified in child operations are relative to the indicated node. The effective target resets to its previous value upon completion of child processing.

| Property | Count | Description |
| - | - | - |
| @optional | 0..1 | Boolean flag indicating whether script syntax errors are fatal (*false*) or merely generate warnings (*true*). Defaults to *false*. |
| @required | 0..1 | Boolean flag indicating whether a failure to uniquely identify an existing output node terminates processing (*true*) or skips processing child operations (*false*). |
| @trace | 0..1 | Label used in trace log output messages. If omitted or empty, the element name is used. |
| @xpath | 1..1 | XPath expression evaluating to an existing output document node. |

See also [ensure-target](#ensure-target) and [if-target](#if-target).

### variable
```xml
    <variable name="String value"
              select="XPath expression"
    />
```

Define an immutable variable for use by the script. It is in scope for any sibling or descendent elements.

| Property | Count | Description |
| - | - | - |
| @name | 1..1 | Required name of the variable. |
| @select | 0..1 | Evaluation of this XPath expression gives the default value of the variable. |
