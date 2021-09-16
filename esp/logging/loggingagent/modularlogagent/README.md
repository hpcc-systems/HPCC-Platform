# Modular Log Agent Plugin

## About

A single log agent plugin can be used to create a single log agent implementation. Support for multiple types of logging (e.g., writing to a file versus updating a database), requires either a plugin for each type or a monolithic agent that tries to do everything. In an effort to reduce the proliferation of plugins, this plugin uses a new monolithic log agent modular framework. Rather than expecting a single class to do everything, the framework breaks down agent functionality into a set of small and possibly reusable components referred to as modules. Access to customer-agnostic implementations of functionality is provided through this plugin. Access to customer-centric implementations should not be included in this plugin; one or more proprietary plugins should be developed to address those requirements.

A logging manager finds occurrences of **LogAgent** within its configuration. Each instance includes a number of properties which can be separated into two categories. Properties defined and used by this agent's framework, i.e., properties that distinguish this agent from others, are referred to as *modular* properties. Properties common to many agents and used by the broader logging engine are referred to as *agent integration* properties.

This document describes how to configure an instance of the plugin for use within the overall logging framework. The intended audience is configuration authors, i.e., individuals responsible for creating configurations using a deployed plugin as opposed to developers maintaining or extending the plugin. Because no previous plugin has required a manual configuration, as this does, limited attention will be given to *agent integration* properties, but the document's focus is on the definition and use of *modular* properties.

A convention used in the document is that within each section that describes a module, one or more subsections are included that contain quoted text in the section heading. These quoted text strings are the plugin-defined selectors used by the plugin to create the configured module implementations. Each module configuration includes an optional *module* attribute that is used to select an implementation. If the attribute is omitted or empty, the *default* implementation is selected. A configuration requesting any *module* value other than those described here is invalid.

### Modules

#### LogAgent

The only module that is required to use the plugin is **LogAgent**. This module provides access to all log agent service functionality. Implementations may be monolithic, including all agent functionality within one class. Alternate implementations may themselves be modular, relying upon smaller, more focused service modules to provide interchangeable functionality.

Monolithic implementations are permitted when they provide the best solution for logging requirements. One plugin may still be configured for multiple types of logging. Modular implementations are preferred due to increased flexibility and reuse.

##### "default"

The default implementation relies upon separate service module implementations for each supported agent service. The use of service modules increases plugin flexibility, especially as the list of supported services expands.

Configuring a supported service module implicitly enables that service. Alternatively, to support YAML property files, a default service module implementation may be requested with a Boolean property in place of an empty element. Omission of a service module configuration explicitly disables that service.

Of the defined log agent services, only one - UpdateLog - is supported. Support for the remaining services will be added in future updates.

##### "mock"

A mocked agent can be configured to produce any response that another agent may produce, with no depenencies on input or external sources. Each service for which the configuration defines a response is enabled. Each service for which the configuration does not define a response is disabled.

Although intended primarily for use with unit tests, exposing the module through the plugin causes no harm and may provide value for testing that is not part of the unit tests.

#### UpdateLog

Implementations of the **UpdateLog** service module provide log update request processing. Implementations may be monolithic, handling update requests from end to end. Alternate implementations may themselves be modular, separating generic functionality from implementation details associated with various forms of persistence (e.g., database, file, or web service updates).

The input to this module will be log content data supplied to the log agent by the log thread. All data transformations may assume this as a starting point. However, the existence of content filters in the manager and agent configurations, as well as custom transformations in the transactional ESP, may radically alter the input. It is the configuration author's responsibility to ensure that meets expectations.

##### "default"

The default implementation relies upon separate **Target** delegate module implementations to persist data prepared by the service module. This creates a logical distinction between the preparation of data to be persisted, which is the responsibility of this module, and the persistence of data, which is the responsibility of **Target** modules. It separates the generic manipulation of update request data from the details of persisting the data.

Each target has expectations of the data provided by the service module. It is the configuration author's responsibility to ensure that the data prepared by the configured service module meets the requirements of the configured delegate module.

Configuring a delegate module implicitly enables data persistence. Omission of a target module configuration limits data persistence to conditional inclusion in trace log output.

#### Target

Implementations of the **Target** delegate module interface persist data supplied by an **UpdateLog** service module. Three data inputs may be provided to each instance, including the complete update request (the *original* input), the possible output of custom transformations applied to the complete update request (the *intermediate* input), and the possible output of an XSL transformation applied to either of the first two inputs (the *final* input). Each implementation defines which inputs it uses and how it uses them. It is the configuration author's responsibility to ensure that the **UpdateLog** module produces the inputs required by the implementation.

##### "default"

The default implementation accepts all content and conditionally adds it to trace log output. This functionality is provided as a development and testing aid.

##### "file"

File logging operates in either a debug or release mode, with the release mode enabled by default. In release mode, the implementation writes the *final* input into a file. In debug mode, the implementation writes all provided inputs into a file. In both modes, filepath variable values are obtained from the *original* input.

A small number of well-known values included in an update request may be referenced in the *filepath* property definition. Output files need to be differentiated by the ESP service instance that created the transaction instead of the ESP service instance processing the log update. The use of dynamic substitutions instead of hard coded configuration values improves the reusability of configurations. Refer to the **Configuration Reference** for details of which values are available.

It is possible, if not likely, that one instance of this module will be expected to manage content in multiple files. The use of variable substitutions in the filepath makes it conceivable that every transaction could be written to a new file. To avoid unconstrained use of system resources, a limit on the number of files a module instance has open at one time is imposed through the configuration. It should only be necessary to change the default value when more than one file is expected. For example, multiple files would be expected if the *filepath* property includes a reference to the ESDL method name.

To prevent output files from growing to a difficult to manage size, file rollover criteria can be given. Rollover on date change, a pattern seen elsewhere in the platform, is enabled by default. This may be insufficient for heavily used services, which is why rollover by file size may be enabled.

## Usage

The modular log agent supports two configuration scenarios.

In the ***inline configuration*** scenario, an instance of **LogAgent** contains all available information for the agent. *Modular* and *agent integration* properties coexist in the same instance. Everything is in one place; the configuration is ***inline***.

In the ***split configuration*** scenario, an instance of **LogAgent** contains the *agent integration* properties plus a reference to a separate file specifying *modular* properties. One configuration is derived from multiple sources; the configuration is ***split***.

Logging configurations are presented to an ESP from multiple sources. Each configuration scenario is optimized for one or more of these sources, but may be applied to each source.

### Legacy

The ESP process may read an *esp.xml* configuration file to configure itself and its services. Traditionally this file is auto-generated from an *environment.xml* file created using *configmgr*. The ability for *configmgr* to configure constructs of which it is unaware is limited. Configurations specified with *configmgr* generally use a split configuration, but an inline configuration may be used to request default agent behavior.

*Configmgr* includes a component named *modularlogagent*. This component is used to configure agents from any plugin using the modular agent framework. The modules, and associated *modular* properties, included in each plugin are unknowns. Reuse by multiple plugins requires that little to no *modular* property knowledge exists in the component. The component supports the specification of *agent integration* properties, a split configuration property file, and (to support the default inline configuration option) a trace logging limit.

| Property | Description |
| -------- | ----------- |
| configuration | Optional file path to either an XML or YAML file containing one **LogAgent** element. An empty value implies the use of the default inline configuration. A non-empty value implies the use of a split configuration with all *modular* properties defined in the external file. |
| plugin | Required shared library identifier. Defaulting to *modularlogagent*, this value may be changed to refer to another library (e.g., *scappsmodularlogagent* for this plugin). |
| trace-priority-level | Maximum trace log output message severity to be included in trace log output. The value is ignored unless *configuration* is empty, in which case the limit applies to the agent and all default modules. |

It is possible to start an ESP process with a manually edited *esp.xml* configuration file, and doing so can be useful for debugging. A split configuration could be replaced with an inline equivalent, but this should not be necessary and is discouraged.

### ESP as an Application

The ESP process, rather than reading a traditional *esp.xml* file, may read an assortment of YAML property files and assemble a configuration equivalent to *esp.xml*. Because this assembly process will understand the relationship of any log agent to a manager, and a manager to an ESP, the assembled configuration could be an inline configuration.

This static binding configuration is meant to be limited to user values for essential process setup parameters. Logging configurations are not essential. An end user may consider logging essential to their deployment of an ESP, but the ESP itself is capable of operating without it. Inline configurations are supported for backward compatibility, but split configurations (or an as yet undefined third configuration scenario) may be preferred.

### Dynamic ESDL Binding

In addition to a logging manager configured by either **Legacy** or **ESP as an Application**, the dynamic ESDL service can use managers configured in the dynamic service bindings loaded at runtime. These configurations should be inline, as split configurations may introduce unwanted cross-dependencies between services.

At the time of this writing, the binding XML must be manually edited. Support for automated assembly of bindings from smaller, reusable property files may be provided in the future.

## Sample Configurations

These examples show only mandatory *agent integration* and key *modular* properties. The illustration of optional *agent integration* properties is beyond the scope of this document.

Each example provides both XML and YAML sample markup for inline configurations. With limited exceptions, the samples represent external property files content used by split configurations. These exceptions are addressed on a case by case basis.

### Default "no-logging" Agent

Conditionally append log update data to trace log output. The condition to ensure output is an ESP process *logLevel* property value of at least *9*.

#### XML

Either:

    <LogAgent name="default-no-logging" plugin="scappsmodularlogagent" UpdateLog="true"/>

or:

    <LogAgent name="default-no-logging" plugin="scappsmodularlogagent">
      <UpdateLog/>
    </LogAgent>

When originating from *configmgr*, if the *environment.xml* configuration omits both *configuration* and *trace-priority-level* properties, the second form is generated in *esp.xml*. No external property file is used.

#### YAML

    LogAgent:
      name: default-no-logging
      plugin: scappsmodularlogagent
      UpdateLog: true

### Verbose "no-logging" Agent

Conditionally append log update data to trace log output. The condition to ensure output is a *trace-priority-limit* property value of at least *90*. Unlike in the **Default "no-logging" Agent** example, this agent controls its own trace output independent of the ESP process and any other configured agent.

#### XML

Either:

    <LogAgent name="verbose-no-logging" plugin="scappsmodularlogagent" trace-priority-limit="90" UpdateLog="true"/>

Or:

    <LogAgent name="verbose-no-logging" plugin="scappsmodularlogagent" trace-priority-limit="90">
      <UpdateLog/>
    </LogAgent>

When originating from *configmgr*, if the *environment.xml* configuration omits the *configuration* property, the second form is generated in *esp.xml*. No external property file is used.

#### YAML

    LogAgent:
      name: verbose-no-logging
      plugin: scappsmodularlogagent
      trace-priority-limit: 90
      UpdateLog: true

### Terse "no-logging" Agent

Refine the output produced by the **Verbose "no-logging" Agent** example by quieting the **LogAgent** and **UpdateLog** modules, and only ensuring recording the **Target** module input. As the complexities of the default modules, especially **UpdateLog**, increases, this example could reduce potentially excessive trace output.

#### XML

    <LogAgent name="terse-no-logging" plugin="scappsmodularlogagent" trace-priority-limit="10">
      <UpdateLog>
        <Target trace-priority-limit="90"/>
      </UpdateLog>
    </LogAgent>

#### YAML

    LogAgent:
      name: terse-no-logging
      plugin: scappsmodularlogagent
      trace-priority-limit: 10
      UpdateLog:
        Target:
          trace-priority-limit: 90

### Disabled UpdateLog

Assuming a fully configured agent including an explicit **UpdateLog** specification, it may be necessary to temporarily prevent the agent from processing update requests. Removing **UpdateLog** would accomplish this, but it may be preferable to retain and disable it.

#### XML

    <LogAgent name="file-logging" plugin="scappsmodularlogagent">
      <UpdateLog disabled="true">
        <Target module="file" filepath="<your-output-file>"/>
      </UpdateLog>
    </LogAgent>

#### YAML

    LogAgent:
      name: file-logging
      plugin: scappsmodularlogagent
      UpdateLog:
        disabled: true
        Target:
          module: file
          filepath: <your-output-file>

### Disabled Target

Assuming a fully configured agent including an explicit **Target** specification, it may be appropriate to allow **UpdateLog** to process a request while preventing **Target** from acting on the results. When more content processing exists in **UpdateLog** than in **Target**, validation of **UpdateLog** changes without persisting the results is a helpful debugging tool.

#### XML

    <LogAgent name="file-logging" plugin="scappsmodularlogagent">
      <UpdateLog>
        <Target module="file" filepath="<your-output-file>" disabled="true"/>
      </UpdateLog>
    </LogAgent>

#### YAML

    LogAgent:
      name: file-logging
      plugin: scappsmodularlogagent
      UpdateLog:
        Target:
          module: file
          filepath: <your-output-file>
          disabled: true

### Generic File Logging

Write unprocessed log update data to a file. The use of the *debug* property is necessary to produce content, and will remain so until data transformations are implemented in **UpdateLog**.

#### XML

    <LogAgent name="file-logging" plugin="scappsmodularlogagent">
      <UpdateLog>
        <Target module="file" filepath="<your-output-file>" debug="true"/>
      </UpdateLog>
    </LogAgent>

#### YAML

    LogAgent:
      name: file-logging
      plugin: scappsmodularlogagent
      UpdateLog:
        Target:
          module: file
          filepath: <your-output-file>
          debug: true

## Configuration Reference

All *agent integration* and *modular* properties may be defined using either XML or YAML markup. This reference uses XML and XPath terminology to describe the properties.

### //LoggingManager/LogAgent

#### *Agent Integration* Properties

All *agent integration* properties are either properties or descendents of **LogAgent**. *Agent integration* properties do not overlap *modular* properties, with the exception of *LogAgent/@name*. An agent name is required in inline configurations and is optional in the external property file used with a split configuration.

| Key | Component | Usage |
| --- | --------- | ----- |
| M | Logging Manager | Reads the value, but does not require it. |
| m | Logging Manager | Requires the value. |
| T | Log Thread | Reads the value, but does not require it. |
| S | Log FailSafe | Reads the value, but does not require it; the utility is used by the log thread. |
| A | Log Agent | Reads the value, but does not require it. |
| a | Log Agent | Requires the value. |
| F | Log Content Filter | Reads the value, but does not require it; the utility is used by the log agent. |

The preceding table is a legend for values in the *Usage* column in the following table. Each key value indicates both the component that uses the value and whether the component requires a value to exist. A reader interested in creating configurations without understanding the implementation need only focus on the case of the key value - upper case identifies an optional value and lower case identifies a required value.

Some properties identify two XPaths, one an attribute and one an element. Historically, these properties were defined as elements. With this plugin, they may now be defined as attributes. XML configurations work well with either form, but element notation is not used in YAML configurations. Both forms are supported, with attributes taking precedence over elements.

| XPath | Usage | Description |
| ----- | ----- | ----------- |
| @name | m, a | Agent instance identifier required by the manager. It is used by multiple components for trace log output. Agent variant identification implicitly assumes the value is a unique identifier, based on bare-metal configurations generated using *configmgr*, but uniqueness is not enforced. |
| @plugin | m | Complete or base shared library name. If the base name is *modularlogagent*, the complete name might be *libmodularlogagent.so*. |
| @type | M | Secondary value used by some other agents for trace log output. Unused by the modular log agent. |
| @AckedFiles / ~~AckedFiles~~ | T | Path to a file listing FailSafe log files that have been acknowledged by an agent instance. Omitted or empty defaults to *AckedFiles*. |
| @AckedLogRequests / ~~AckedLogRequests~~ | T | Path to file listing the log update request GUIDs that have been acknowledged by an agent instance. Omitted or empty defaults to *AckedLogRequests*. |
| @DecoupledLogging / ~~DecoupledLogging~~ | S | Boolean value indicating whether the agent uses decoupled logging (*true*) or not (*false*). This is a manager-specific property read by a utility class shared by the manager and thread. It *must not* be set in an agent configuration. |
| @DisableFailSafe / ~~DisableFailSafe~~ | T | Boolean value indicating whether agent-specific FailSafe logging configurations should be ignored (*true*) or honored (*false*). It is meant to be *true* when the logging manager is managing FailSafe log files. |
| @FailSafe / ~~FailSafe~~ | T | Boolean value indicating whether the agent requests per-agent FailSafe log files (*true*) or not (*false*). |
| @FailSafeLogsDir / ~~FailSafeLogsDir~~ | S | Directory in which FailSafe log files are created. Omitted or empty defaults to *./FailSafeLogs*. |
| Filters/Filter | F | Repeating element used to prune the log request XML document. Each element may specify an XPath into the complete document, or an XPath into a specific section of a standard *UpdateLogRequest* XML document. Contents of the specified paths are retained, while all others are removed. All content is retained if no filters are specified; filters are only necessary if the intent is to not retain some part or parts of the content. |
| Filters/Filter/@type | F | Identifier of the UpdateLogRequest/LogContent child element affected by the correseponding **value**. It may be either blank, *ESPContext*, *UserContext*, *UserRequest*, *UserResponse*, *BackEndResponse*. If blank, the corresponding **value** applies to the complete document. If *BackEndResponse*, **value** is ignored and the entire section is retained. |
| Filters/Filter/@value | F | XPath relative to the indicated document section, or complete document, of an element to be retained in the log update request. If blank, the entire section is retained. |
| @MaxLogQueueLength / ~~MaxLogQueueLength~~ | T | Maximum log update request queue length. It is an error to attempt to add more than this number of requests to the request queue. Omitted or empty defaults to *500,000*. |
| @MaxRetriesRS / ~~MaxRetriesRS~~ | T | Maximum number of attempts to resent a log update request if sending fails. Omitted or empty defaults to *-1*, which has the effect of not limiting the attempts. |
| @PendingLogBufferSize / ~~PendingLogBufferSize~~ | T | Maximum number of pending log update requests that can be queued before moving them into the main request queue. Omitted or empty defaults to *100*. |
| @QueueSizeSignal / ~~QueueSizeSignal~~ | T | Defines a log update request queue size threshold for which a trace log error message is generated when the queue size reaches any multiple of the threshold. If set to *5,000*, then an error message will be recorded when the queue size reaches *5,000*, *10,000*, *15,000*, etc. Omitted or empty defaults to *10,000*. |
| @ReadRequestWaitingSeconds /~~ReadRequestWaitingSeconds~~ | T | Polling interval, in seconds, for FailSafe log file update checks. Omitted or empty defaults to *15*. |
| @SafeRolloverThreshold / ~~SafeRolloverThreshold~~ | S | Maximum size of a FailSafe log file before it is closed and a new, empty file is created in its place. The format of the value is a scalar followed by an optional unit. If the unit is omitted, the scalar represents a number of bytes. With units of *K*, *M*, *G*, or *T*, the scalar represents a number of kilobytes, megabytes, gigabytes, or terabytes. In reality, the total number of bytes should not exceed four gigabytes due to internal limits. Omitted or empty defaults to *0*, implying no size constraint. |
| Variant | A | Repeating element containing user-defined identifiers for an agent. ESDL services which configure agent instances that do not apply to every transaction may reference these identifiers in custom transforms to manage which agents are applied to a transaction. |
| Variant/@type | A | Identifier of a single kind of log content generated by the agent. An agent that will not be controlled by content kind may omit this identifier. An agent that will be controlled by content kind should specify a unique identifier for each kind of content to be controlled. In custom transform parlance, this concept maps to a log option. |
| Variant/@group | A | Identifier for a collection of agents that are used together. Given a service with agents A, B, C, and D, some service methods may only use A and B, while others may only use C and D. A and B can be grouped by name, as can C and D, such that a new agent E may be seamlessly added to the group of A and B. In custom transform parlance, this concept maps to a log profile. |

In the case of a split configuration, these properties must exist in the base configuration. Repeating them in the external property file will cause no harm, but externally defined values will have no effect on the logging manager or other logging components.

#### *Modular* Properties

| XPath | Description |
| ----- | ----------- |
| @configuration | Optional path to an external configuration file. If omitted or empty, the configuration is inline. If not empty, the configuration is split. |
| @module | Optional plugin-defined identifier for which **LogAgent** implementation to use. Omitted or empty assume *default*. In a split configuration, only the value in the external property file is honored. |
| @name | Optional user-defined identifier used in trace log output. In an inline configuration, the value is required as an *agent integration* property. In a split configuration and in the external property file, the value is not required to match the value of the corresponding *agent integration* property. |
| @trace-priority-level | Optional value of the maximum trace log output message severity to be included in trace log output, given on a scale of *-1* to *100*. Omitted or empty assumes *-1*, which inherits a limit derived from the ESP log level. Honored in an inline configuration; ignored in a split configuration. |

#### "default" Implementation

| XPath | Description |
| ----- | ----------- |
| @UpdateLog | Optional Boolean flag instructing the agent to use either the *default* **UpdateLog** or no **UpdateLog**. It must not be combined with an **UpdateLog** child property. In the absence of **UpdateLog**, an omitted, empty, or *false* value prevent log update requests from being processed. |
| UpdateLog | Optional configuration of a module intended to process update log requests. It must not be combined with an **@UpdateLog** property. In the absence of **@UpdateLog**, omission prevents log update requests from being processed. |

The default **UpdateLog**, described in the next section, has little to no practical value in a live deployment. In fact, writing potential PII to trace log output should be seen as a security risk. It should not be enabled by default.

The default **UpdateLog** does add value in development and testing environments. It enables verification of the logging framework, confirming that the expected data reaches the intended agents. It also provides verification that ESDL transformations performed by the transactional ESP produce the expected content.

If one assumes that an empty **UpdateLog** element implies the default behavior, because it contains no directions to deviate from its default behavior, one could also assume that including an empty element is sufficient to request the default behavior. This assumption is valid with XML markup. YAML markup doesn't handle empty elements, at least not well.

For YAML compatibility, an **@UpdateLog** scalar value is defined. A YAML property file may specify *UpdateLog: true* to enable the default behavior. A property file may specify either **@UpdateLog** or **UpdateLog**, but not both. Including both is an error and the log update service will not be available.

#### "mock" Implementation

| XPath | Description |
| ----- | ----------- |
| GetTransactionSeed | Optional element to enable the *GetTransactionSeed* service. The return value of `getTransactionSeed` is derived from the value of `@status-code`. |
| GetTransactionSeed/@seed-id | Optional value to populate IEspGetTransactionSeedResponse::SeedId. |
| GetTransactionSeed/@status-code | Optional value to populate IEspGetTransactionSeedResponse::StatusCode. |
| GetTransactionSeed/@status-message | Optional value to populate IEspGetTransactionSeedResponse::StatusMessage |
| GetTransactionId | Optional element to enable the *GetTransactionID* service. |
| GetTransactionId/@id | Optional value to populate the `transactionID` parameter. |
| UpdateLog | Optioonal element to enable the *UpdateLOG* service. |
| UpdateLog/@response | Optional value to populate IEspUpdateLogResponse::Response. |
| UpdateLog/@status-code | Optional value to populate IEspUpdateLogResponse::StatusCode. |
| UpdateLog/@status-message | Optional value to populate IEspUpdateLogResponse::StatusMessage. |

### //LogAgent/UpdateLog

#### *Modular* Properties

| XPath | Description |
| ----- | ----------- |
| @disabled | Optional Boolean flag allowing a configured module to be ignored (*true*) or honored (*false*). Omitted or empty defaults to *false*. |
| @module | Optional plugin-defined identifier for which **UpdateLog** implementation module to use. Omitted or empty assumes "default". |
| @name | Optional module identifier used in trace log output. No default is applied. |
| @trace-priority-level | Optional value of the maximum trace log output message severity to be included in trace log output, given on a scale of *-1* to *100*. Omitted or empty assumes *-1*, which inherits from **LogAgent**. |

#### "default" Implementation

| XPath | Description |
| ----- | ----------- |
| Target | Optional configuration of a content destination used to complete a log update. Absence of the element causes the text buffer that would have been sent to a configured **Target** module to be recorded in trace log output, with priority *90*. Presence of the element causes the text buffer produced by this module to be passed to the configured **Target** module. |

### //UpdateLog/Target

#### *Modular* Properties

| XPath | Description |
| ----- | ----------- |
| @disabled | Optional Boolean flag allowing a configured module to be ignored (*true*) or honored (*false*). Omitted or empty defaults to *false*. |
| @module | Optional plugin-defined identifier for which **Target** implementation module to use. Omitted or empty assumes "default". |
| @name | Optional module identifier used in trace log output. No default is applied. |
| @trace-priority-level | Optional value of the maximum trace log output message severity to be included in trace log output, given on a scale of *-1* to *100*. Omitted or empty assumes *-1*, which inherits from **UpdateLog**. |

#### "default" Implementation

N/A

#### "file" Implementation

| XPath | Description |
| ----- | ----------- |
| @filepath | Required filesystem path specifying the location of the log file. While the given value may be a valid path, it may be given as a pattern that includes one or more variable substitutions. Variable substitutions are described in depth immediately following this table. |
| @module | Required module key label used to select file logging. The value must be *file*. |
| @concurrent-files | Optional upper bound for the number of open files managed by instance at any given time. Variable substitions in **@filepath** make multiple paths possible, and this value prevents unbounded resource consumption. If omitted or empty, the value is assumed to be *1*. The value must be between 1 and 255.
| @debug | Optional Boolean flag to control whether only the *final* input (*false*) or all (*true*) input is written to the file. If omitted or empty, the value is assumed to be *false*.
| @format-creation-date | Optional override of the default date format string used when *creation:date* is included in **@filepath**. If omitted or empty, the value is assumed to be "%Y_%m_%d". When given, the value must include "%Y", "%m", and "%d" for strftime formatting. |
| @format-creation-time | Optional override of the default time format string used when *creation:time* is included in **@filepath**. If omitted or empty, the value is assumed to be "%H_%M_%S". When given, the value must include "%H", "%M", and "%S" for strftime formatting. |
| @format-creation-datetime | Optional override of the default date and time format string used when either *creation* or *creation:datetime* is included in **@filepath**. If omitted or empty, the value is assumed to be "%Y_%m_%d_%H_%M_%S". When given, the value must include "%Y", "%m", "%d", "%H", "%M", and "%S". |
| @rollover-interval | Optional specification of the frequency at which a new file is automatically created, regardless of the size of the current file. Must be either "none", indicating no timed rollover, or "daily", indicating a rollover on a date change. If omitted or empty, the value is assumed to be "daily". A rollover file will only be created to record a transaction's data; an instance with no activity for a complete calendar day should create no file for that calendar day. |
| @rollover-size | Optional specification of the maximum size for a file before a new file is automatically created. If omitted or empty, the value is assumed to be "0". A value of zero disables size-based rollover; a positive number by itself is bytes; a positive number followed by 'k' or 'K' is kilobytes; a positive number followed by 'm' or 'M' is megabytes; a positive number followed by 'g' or 'G' is gigabytes; a positive number followed by 't' or 'T' is terabytes; any other value is invalid. Each file is guaranteed to contain at least one complete transaction regardless of the configured constraint. |
| @trace-priority-limit | Optional upper bound on which trace output entries will be recorded, given on a scale of *-1* to *100*. If omitted or empty, the value is assumed to be *-1*. A value of *-1* instructs the module to inherit its constraint from the containing **UpdateLog** instance. |

Variable substitutions enable an instance to dynamically adjust the location and name of an output file based on information obtained from the UpdateLogRequest XML document. This flexibility may not be crucial for dynamic binding configurations when most of the values are known, but it is needed for configurations that cannot easily be tailored to a binding. Variables are represented in **@filepath** using the "{$" `<name>` [ ":" `<option>` ] "}"' markup. All variable references must specify a name, while most will not include an option. The accepted variables are:

| Name | Option | Description |
| ---- | ------ | ----------- |
| binding | N/A | Substitutes the dynamic ESDL binding ID, which is comprised of the process name, port number, and ESDL service name. |
| creation | | Substitutes the current date and time at the moment of file creation. An empty option is equivalent to the *datetime* option. |
| creation | custom | For the first file created by an instance, substitutes the current date at the moment of file creation. If a file exists with the substituted value, it substitutes the current date and time at the moment of file creation. |
| creation | date | Substitutes the current date at the moment of file creation. |
| creation | datetime | Substitutes the current date and time at the moment of file creation. |
| creation | time | Substitutes the current time at the moment of file creation. |
| creation | `<format>` | Substitutes the current date or time at the moment of file creation, according to the strftime format string specified. |
| esdl-method | N/A | Substitutes the ESDL-defined method name. |
| esdl-service | N/A | Substitutes the ESDL-defined service name. |
| port | N/A | Substitutes the port number on which the transaction occurred. |
| process | N/A | Substitutes the configured process name from which the transaction occurred. |
| service | N/A | Substitutes the configured service name from which the transaction occurred. |

Given a transactional process named "myesdlesp" and a service named "myservice", a **@filepath** value of */var/log/HPCCSystems/{$process}/{$service}/log.{$creation:custom}.csv* could resolve to */var/log/HPCCSystems/myesdlesp/myservice/log.2020_12_31.csv* for a transaction occurring on December 31, 2020.
