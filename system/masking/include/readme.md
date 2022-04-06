# Data Masking Framework

This is a high level description of the data obfuscation framework defined in `system/masking/include`. The framework includes a platform component, the engine, that exposes obfuscation logic supplied by dynamically loaded plugin libraries. The framework can be used to obfuscate sensitive data, such as passwords and PII. Possible use cases including preventing trace output from revealing sensitive values and partially masking values in situations where a user needs to see "enough" of a value to confirm its correctness without seeing all of the value (e.g., when a user is asked to confirm the last four digits of an account number).

| File | Contents |
| - | - |
| datamasking.h | Declares the core framework interfaces. Most are used by the engine and plugins. |
| datamaskingengine.hpp | Defines the engine component from the core engine interface. |
| datamaskingplugin.hpp | Defines a set of classes derived from the core plugin interfaces that can be used to provide rudimentary obfuscation. It is expected that the some or all classes will be subclassed, if not replaced outright, in new plugins. |
| datamaskingshared.hpp | Defines utilities shared by engine and plugin implementations. |

## Glossary

### Domain

A *domain* is a representation of the obfuscation requirements applicable to a set of data.

Consider that the data used to represent individuals likely differs between countries. With different data, requirements for obfuscation may reasonably be expected to vary. Assuming that requirements do change between countries, each country's requirements could logically constitute a separate domain. The capacity to define multiple *domains* does not create a requirement to do so.

Requirements can change over time. To support this, a *domain* can be seen as a collection of requirement snapshots where each snapshot defines the complete set of requirements for the *domain* at a point in time. Snapshots are referenced by unique version numbers, which should be sequential starting at *1*.

Obfuscation is always applied based on a single snapshot of a domain's requirements.

*Domains* are represented in the framework interface as text identifiers. Each distinct *domain* is identified by at least one unique identifier.

### Masker

A *masker* is a provider of obfuscation for a single snapshot of a *domain's* requirements. There are three masking operations defined in this framework. Each instance decides which of the three it will support, and how it will support them. The three operations are:

 - `maskValue` obfuscates individual values based on snapshot-defined meanings. For example, a value identified as a password might require complete obfuscation anywhere it appears, or an account number may require complete obfuscation in some cases and partial obfuscation in others.
 - `maskContent` obfuscates a variable number of values based on context provided by surrounding text. For example, the value of an HTTP *authentication* header or the text between `<Password>` and `</Password>` might require obfuscation. This operation can apply to both structured and unstructured text content.
 - `maskMarkupValue` obfuscates individual values based on their locationa within an in-memory representation of a structured document, such as XML or JSON. For example, the value of element `Password` might require obfuscation unconditionally, while the value of element `Value` might require obfuscation only if a sibling element named `Name` exists with value *password*. This operation relies on the caller's ability to supply context parsed from structured content.

A masker may be either stateless or stateful. With a stateless masker, identical input will produce identical output for every requested operation. A stateful masker, however, enables its user to affect operation outputs (i.e., identical input may not produce identical output for each operation).

*Maskers* are represented in the framework interface using `IDataMasker`, with `IDataMaskerInspector` providing access to less frequently used information about the *domain*.

### Profile

A *profile* is a stateless *masker*. Each instance defines the requirements of one or more snapshots of a single *domain*.

Snapshots are versioned. Each *profile* declares a minimum, maximum, and default version. *Masker* operations apply to the default version. Other declared versions may be accessed using a stateful *context*, which the *profile* can create on demand.

Each instance must support at least one version of a *domain's* requirements. Whether an instance supports more than one version depends on the implementation and on user preference. A *domain* can be viewed as a collection of one or more *profiles* where each *profile* defines a unique set of requirement snapshots applicable to the same underlying data.

Refer to `IDataMaskingProfile` (extending `IDataMasker`)  and `IDataMaskingProfileInspector` (extending `IDataMaskerInspector`) for additional information.

### Context

A *context* is a stateful *masker*. Instantiated by and tightly coupled to a *profile*, it provides some user control over how masking operations are completed.

- For a *profile* supporting multiple versions, the requirements of a non-default version may be applied.
- Custom properties, defined by a *profile*, may be managed.
  - `valuetype-set` is a pre-defined property used to select the group of *value types* that may be masked by any operation.
  - `rule-set` is a pre-defined property used to select the group of *rules* that will be applied for `maskContent` requests.
  - *Profile* implementations may define additional properties as needed. For example, one might define `mask-pattern` to override the default obfuscation pattern to avoid replicating (and complicating) configurations just to change the appearance of obfuscated data.
- Trace output produced by operation requests, including errors and warnings encountered during request processing, can be controlled per context. This does not affect the operation output, per-se, but provides compatibility with transactional trace output control.

Refer to `IDataMaskingProfileContext` (extending `IDataMasker`) and `IDataMaskingProfileContextInspector` (extending `IDataMaskerInspector`) for additional information.

### Value Type

Each snapshot defines one or more *value types*. A *value type* is a representation of the requirements pertaining to a particular concept of a *domain* datum. Requirements include:

- instructions for identifying occurrences based on contextual clues found in a body of text; and
- instructions for applying obfuscation to an identified value.

A Social Security Number, or SSN, is a U.S.-centric datum that requires obfuscation and for which a *value type* may be defined. Element names associated with an SSN may include, but are not limited to, *SSN* and *SOCS*; the *value type* is expected to identify all such names used within the *domain*. SSN occurrences are frequently partially masked, with common formats being to mask only the first four or the last five digits of the nine digit number; the *value type* defines which formats are available besides the default of masking all characters.

*Value types* are represented in the framework using `IDataMaskingProfileValueType`. They are accessed through the `IDataMaskerInspector` interface.

### Mask Style

A *mask style* describes how obfuscation is applied to a value. It is always defined in the context of a *value type*, and a *value type* may define multiple.

A *value type* is not required to define any *mask styles*. If none are defined, all value characters are obfuscated. If the requested *mask style* is not defined, the default obfuscation occurs; the *value type* will not attempt to guess which of the defined styles is appropriate.

*Mask styles* are represented in the framework using `IDataMaskingProfileMaskStyle`. They are accessed through the `IDataMaskingProfileValueType` interface.

### Rule

A *rule* contains the information necessary to locate at least one occurrence of a *value type* datum to be obfuscated. It is always defined in the context of a *value type*, and a *value type* may define multiple.

*maskValue* requests do not use *rules*. *maskContent* requests rely on *rules* for locating affected values, with the relationship between a *profile* and its *rules* an implementation detail. *maskMarkupValue* requests, when implemented, may also use *rules* or may take an entirely different approach.

*Rules* are represented in the framework as an abastract concept that cannot be inspected individually. Inspection may be used to establish the presence of *rules*, but not to examine individual instances.

### Plugin

The combination of a shared library and entry point function describes a *plugin*. The input to a *plugin* is a property tree describing one or more *profiles*. The output of an entry point function is an iterator of *profiles*. The *profiles* created by the function may all be associated with the same *domain*, but this is not required.

*Plugin* results are represented in the framework using `IDataMaskingProfileIterator`.

### Engine

An *engine* is the platform's interface to obfuscation. It loads *domains* by loading one or more *plugins*. *Plugins* yield *profiles*, from which *domains* are inferred.

Once configured with at least one *domain*, a caller can obtain obfuscation in multiple ways:

1. As an instance of a stateless *masker*, an *engine* can provide obfuscation. The default version of the default *profile* of the default *domain* will be used, and no custom *context* properties can be used. This is the simplest integration, appropriate for use when the host process implicitly trusts that the default configuration is sufficient.
2. An *engine* may be used to obtain a stateless *profile* for a specific *domain*. If no version is requested, the default *profile* of the requested *domain* will be used. If a version is specified, the *domain* *profile* supporting the requested version is used. This usage supports host processes that are aware of *domains* and need to use specific instances.
3. An *engine* may be used to obtain a *context* tied to a specific version of a *domain*. The default *domain* may be requested with an empty string. The default version may be requested by passing *0*. With a *context*, a caller may manipulate further the environment for obfuscation requests.

*Engines* are represented in the framework using `IDataMaskingEngine` (extending `IDataMasker`) and `IDataMaskingEngineInspector` for additional information.

## Environment

This section assumes a *context* is in use. Absent a *context*, only the default state of the default version of a *profile* can be used.

The framework reserves multiple custom property names, which are described in subsequent subsections. It also allows implementations to define additional properties using any non-reserved name.

Suppose an implementation defines a property to override the default obfuscation pattern. Let's call this property `default-pattern`. A caller could interrogate a *masker* to know if `default-pattern` is accepted. If accepted, `setProperty("default-patterns", "#")` can be used to register an override.

All custom properties, whether defined in the framework or in third party libraries, are managed using a generic *context* interface. This interface includes:

```
bool hasProperties() cosnt
bool hasProperty(const char* name) const
const char* queryProperty(const char* name) const
bool setProperty(const char* name, const char* value)
bool removeProperty(const char* name)
```

### Value Type Sets

```
bool setProperty(const char* name, const char* value);
```

#### Overview

The abstraction includes the concept of sets of related *value types*. All *value types* should be assigned membership in at least one set. One set should be selected by default. The custom *context* property **valuetype-set** is used to select a different set.

The rationale for this is an expectation that certain data always requires obfuscation. A password, for example, would never not require obfuscation. Other data may only require obfuscacation in certain situations, such as when required by individual customer agreements. Callers should not be required to complete additional steps to act on data that must always be obfuscated, and should not need to know which data falls in which category.

> The set name "\*" is reserved to select all *value types* regardless of their defined set membership. This mechanism is intended to assist with compatibility checks, and should be used with care in other situations.

#### Runtime Compatibility

Use `acceptsProperty` to determine whether a snapshot recognizes a custom property name. Use `usesProperty` to learn if the snapshot includes references to the custom property name. Acceptance implies awareness of the set (and what it represents) even if selecting it will not change the outcome of any masking request. Usage implies that selecting the set will change the outcome of at least one masking operation.

The property `valuetype-set` is used to select a named set. A check of this property addresses whether the concept of a set is accepted or used by the snapshot.

> For improved compatibility checks, implementations are encouraged to reserve additional property names that enable checks for individual set names. For each set name, *foo*, the included implementations report property `valuetype-set:foo` as used.

#### Implementation

The included implementations allow membership in either a default, unnamed set or in any number of named sets. Absent a contextual request for a named set, the unnamed set is selected by default. With a contextual set request, the members of the named set are selected in addition to members of the unnamed set. Members of the unnamed set are never not selected.

> Unnamed set membership cannot be defined explicitly. Because this set is always selected, assigning a *value type* to both a named and the unnamed set is redundant - the type will be selected whether the named set is requested or not.

*Example 1a: default value type*

```
profile:
  valueType:
    - name: type1
```

*Value type* `type1` belongs to the default, unnamed *value type* set. Property `valuetype-set` is accepted, but not used; an attempt to use it will change no results.

*Example 1b: sample set memberships*

```
profile:
  valueType:
    - name: type1
    - name: type2
      memberOf:
        - name: set1
        - name: set2
    - name: type3
      memberOf:
        - name: set2
        - name: set3
    - name: type4
      memberOf:
        - name: set1
    - name: type5
      memberOf:
        - name: set3
```

Extending the previous example, five types and four sets are in use. Property `valuetype-set` is both accepted and used, as it can now impact results. Properties `valuetype-set:set1`, `valuetype-set:set2` and `valuetype-set:set3` are also both accepted and used, but are intended only for use with compatibility checks.

This table shows which types are selected for each requested set:

|  | *unnamed* | set1 | set2 | set3 | \* |
| -: | :-: | :-: | :-: | :-: | :-: |
| type1 | Y | Y | Y | Y | Y |
| type2 | N | Y | Y | N | Y |
| type3 | N | N | Y | Y | Y |
| type4 | N | Y | N | N | Y |
| type5 | N | N | N | Y | Y |

*Example 1c: sample accepted sets*

```
profile:
  valueType:
    - name: type1
    - name: type2
      memberOf:
        - name: set1
        - name: set2
    - name: type3
      memberOf:
        - name: set2
        - name: set3
    - name: type4
      memberOf:
        - name: set1
    - name: type5
      memberOf:
        - name: set3
  property:
    - name: 'valuetype-set:set4'
    - name: 'valuetype-set:set5'
```

Continuing the previous example, the *profile* has declared acceptance of two additional sets using properties `valuetype-set:set4` and `valuetype-set:set5`. Selection of these sets will not change results, but a caller might accept acceptance of a set as sufficient to establish compatibility.

### Rule Sets

```
bool setProperty(const char* name, const char* value);
```

#### Overview

The abstraction includes the concept of sets of related *rules*. All *rules* should be assigned membership in at least one set. One set should be selected by default. The custom *context* property **rule-set** is used to select a different set.

The rationale for this is similar to yet different than that of *value type* sets. Instead of one set of *rules* that are always selected, backward compatibility with a proprietary legacy implementation requires that the default set be replaced by an alternate collection.

> The set name "\*" is reserved to select all *rules* regardless of their defined set membership. This does not override constraints imposed by the current *value type* set. This mechanism is intended to assist with compatibility checks, and should be used with care in other situations.

#### Runtime Compatibility

Use `acceptsProperty` to determine whether a snapshot recognizes a custom property name. Use `usesProperty` to learn if the snapshot includes references to the custom property name. Acceptance implies awareness of the set (and what it represents) even if selecting it will select no rules. Usage implies that selecting the set will select at least one rule.

The property `rule-set` is used to select a named set. A check of this property addresses whether the concept of a set is accepted or used by the snapshot.

> For improved compatibility checks, implementations are encouraged to reserve additional property names that enable checks for individual set names. For each set name, *foo*, the included implementations report property `rule-set:foo` as used.

#### Implementation

The included implementations allow membership in any number of named sets as well as a default, unnamed set. Absent a contextual request for a named set, the unnamed set is selected by default. With a contextual set request, only the members of the requested set are selected.

> Unlike *value type* sets, the unnamed set is not always selected. Because of this difference, a *rule* may be assigned explicit membership in the unnamed set. This is optional for *rules* that belong only to the unnamed set and is required for *rules* meant to be selected as part of the unnamed set and one or more named sets.

*Example 2: rule set memberships*

```
profile:
  valueType:
    name: type1
    rule:
      - name: foo
      - memberOf:
          - name: ''
      - memberOf:
          - name: ''
          - name: set1
      - memberOf:
          - name: set1
  property:
    name: 'rule-set:set2'
```

The rules described here are intentionally incomplete, showing only what is necessary for this example. Four rules are defined:

1. The rule named *foo* implicitly belongs to the unnamed set.
2. The second rule explicitly belongs to the unnamed set.
3. The third rule explicitly belongs to the unnamed set and to `set1`.
4. The fourth rule belongs to `set1`.

property `rule-set` is both accepted and used. Properties `rule-set:` and `rule-set:set1` are both accepted and used, intended for use with compatibility checks. Property `rule-set:set2` is accepted.

## Operations

### maskValue

```
bool maskValue(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const;
bool maskValueConditionally(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length) const;
bool maskValueUnconditionally(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length) const;
```

#### Overview

Given a single *domain* datum, obfuscate the content "as needed". The framework anticipates two interpretations of "as needed", either conditional or unconditional:

- Conditional means that values of a named type are only obfuscated if the named type is known by and selected in the snapshot. This usage assumes that the *profile* is the authoritative source for obfuscation requirements. The *profile*&apos;s consumer may ask for any value to be obfuscated, but the *profile* is not required to act.
- Unconditional means that all value obfuscation requests will result in obfuscation. The named type is not required to be known by or selected in the snapshot. This usage assumes that the *profile*&apos;s consumer is the authoritative source for obfuscation requirements. If a consumer asks for a value to be obfuscated, the *profile* must act.

Each *plugin* will define its own interpretation of "as needed". The API distinction between conditional and unconditional is a hint intended to guide implementations capable of both, and should be ignored by implementations that are not.

The `buffer`, `offset`, and `length` parameters define what is assumed to be a single *domain* datum. That is, every character in the given character range is subject to obfuscation.

The `valueType` parameter determines if obfuscation is required, with `conditionally` controlling whether an unrecognized `valueType` is ignored (*true*) or forced to obfuscate (*false*).

The `maskStyle` parameter may be used to affect the nature of obfuscation to be applied. If the parameter names a defined *mask style*, that obfuscation format is applied. If the parameter does not name a define *mask style*, the *value type's* default obfuscation format is applied.

#### Runtime Compatibility

The `canMaskValue` method of `IDataMasker` can be used to establish whether a snapshot supports this operation. A result of *true* indicates that the plugin is capable of performing obfuscation in response to a request. Whether a combination of input parameters exists that results in obfuscation depends on the definition of the snapshot.

The `hasValueType` method of `IDataMaskerInspector` can be used to check if a given name is selected in the snapshot. The reserved name "\*" may be used to detect unconditional obfuscation support when available in the snapshot. Names defined yet unselected in the snapshot are not directly detectable, but can be detected by comparing results of using multiple *context* configurations.

> The name "\*" is reserved by the abstraction for compatibility checks.

To confirm the availability of a specific *mask style* first requires confirmation of the *value type* to which the *mask style* is related. Use `queryValueType` to obtain the defined instance and, from the result, call `queryMaskStyle` to confirm the existence of the *mask stye*. For each of these, corresponding `get...` methods are defined to obtain new references to the objects. The methods are declared by `IDataMaskerInspector`.

#### Implementations

The included implementations are inherently conditional. Obfuscation depends on matching `valueType` with a selected *value type* instance in the snapshot, where an instance is selected if it belongs to the currently selected *value type* set.

Unconditional obfuscation is enabled in profiles that include a *value type* instance named "\*". If an instance with this name is selected by the currently selected *value type* set, all values for undefined *value types* will be obfuscated. Values for defined but unselected *value types* will not be obfuscated.

- A value of type "foo" will be obfuscated when type "foo" is selected by the currently selected *value type* set.
- A value of type "foo" will be obfuscated when type "foo" is undefined and type "\*" is selected by the currently selected *value type* set.
- A value of type "foo" will not be obfuscated when type "foo" is defined but unselected by the currently selected *value type* set.

*Example 3a: conditional obfuscation*

```
valueType:
  - name: type1
  - name: type2
    memberOf:
      - name: set1
```

This snippet declares two *value types*, with two total sets. The table shows which values will be conditionally obfuscated based on a the combination of `valueType` and the currently selected *value type* set.

| `valueType` | Selected Set | Obfuscated |
| :-: | :-: | :-: |
| type1 | N/A | Yes |
| type1 | set1 | Yes |
| type2 | N/A | No |
| type2 | set1 | Yes |
| typeN | N/A | No |
| typeN | set1 | No |

> "typeN" in the table represents any named type, excluding the reserved "\*", not explicitly defined in the *profile*.

*Example 3b: unconditional obfuscation*

```
valueType:
  - name: type1
  - name: type2
    memberOf:
      - name: set1
  - name: *
```

Extending the previous example with a third *value type*, values of unknown type are now obfuscated. Values of known but unselected type remain unobfuscated.

| `valueType` | Selected Set | Obfuscated |
| :-: | :-: | :-: |
| type1 | N/A | Yes |
| type1 | set1 | Yes |
| type2 | N/A | No |
| type2 | set1 | Yes |
| typeN | N/A | Yes |
| typeN | set1 | Yes |

> The name reserved by the abstraction to detect support for unconditional obfuscation is the same name reserved by the implementation to define that support.

> "typeN" in the table represents any named type, excluding the reserved "\*", not explicitly defined in the *profile*.

### maskContent

```
bool maskContent(const char* contentType, char* buffer, size_t offset, size_t length) const;
```

#### Overview

The `buffer`, `offset`, and `length` parameters define what is assumed to be a blob of content that may contain zero or more occurrences of *domain* data. The blob is expected to contain sufficient context allowing a snapshot's *rules* to locate any included occurrences.

The `context` parameter optionally influences which *rules* are selected in a snapshot, while the `contentType` parameter offers a hint as to the blob's data format. Each snapshot *rule* may be assigned a format to which it applies. Operation requests may be optimized by limiting the number of selected *rules* applied by describing the format. Selected *rules* not assigned a format will be applied in all requests.

> There is no equivalent to the unconditional mode offered by `maskValue`. Content obfuscation is always conditional on matching *rules*.

#### Runtime Compatibility

The `canMaskContent` method of `IDataMasker` can be used to establish whether a snapshot supports this operation. A result of *true* indicates that the plugin is capable of performing obfuscation in response to a request. Whether a combination of input parameters exists that results in obfuscation depends on the definition of the snapshot.

The `hasRule` method of `IDataMaskerInspector` indicates if at least one *rule* is selected in the snapshot for a given content type. *Rules* not associated with any content type may be interrogated using an empty string.

> It is not possible to discern any information about the selected rules, such as their associated *value types* or the cues used to apply them in a buffer.

#### Implementations

The groundwork exists for two types of *rules*, serial and parallel. Serial rules, as the name implies, are evaluated sequentially. Parallel *rules*, on the other hand, are evaluated concurrently. Concurrent evaluation is expected to be more efficient than sequential, but no concurrent implementation is provided.

The included sequential implementation should be viewed as a starting point. Each *rule* defines a start token and an optional end token. For each occurrence of the start token in the blob, a corresponding search for an end token (newline if omitted), is performed. When both parts are found, the content between is obfuscated using the associated *value type's* default *mask style*.

> Traversing a potentially large blob of text once per *rule* is inefficient. A concurrent implementation is in development to improve performance and capabilities. A *domain* originated using the original implementation and migrated to the new implementation illustrates one *domain* implemented by multiple plugins and, by extension, multiple *profiles*.

### maskMarkupValue

```
bool maskMarkupValue(const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const;

```

#### Overview

Where `maskValue` obfuscates individual values based on what the caller believes the value to be, and `maskContent` obfuscates any number of values it identifies based on cues found in surrounding text, `maskMarkupValue` obfuscastes individual values based on cues not provided to it.

Given a value and a relative location within a structure document, an implementation must decide whether obfuscation is required, may be required, or is not required. If required, it can obfuscate immediately. If not required, and can return immediately. If it might be required, the implementation must request the context it needs from the caller in order to make a final determination.

A request to mask the content of an element named `Value` might depend on the content of a sibling element named `Name`. If value of `Value` probably does not require obfuscation when the value of `Name` is *city*, but almost certainly does require obfuscation when the value of `Name` is *password*. When processing the request for `Value`, an implementation must ask for the value of a sibline named `Name` to decide whether or not to obfuscate the data.

> There is no equivalent to the unconditional mode offered by `maskValue`. Markup value obfuscation presumes that the caller is traversing a structured document without awareness of which values require obfuscation. If the caller knows which values require obfuscation, it should use `maskValue` on those specific values.

#### Runtime Compatibility
```
    virtual bool getMarkupValueInfo(const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const = 0;
```

The `canMaskMarkupValue` method of `IDataMasker` can be used to establish whether a snapshot supports this operation. A result of *true* indicates that the plugin is capable of performing obfuscation in response to a request. Whether a combination of input parameters exists that results in obfuscation depends on the definition of the snapshot.

The `getMarkupValueInfo` method of `IDataMaskerInspector` determines if the value of an element or attribute requires obfuscation. If required, the `info` parameter will describe on completion how to perform the obfuscation using `maskValue` instead of `maskMarkupValue`. This is done to optimize performance by eliminating the need to reevaluate rules to re-identify a match.

Assme a *value type* representing passwords is defined. If given an element value containing a URL with an embedded password, obfuscation is required. But most likely not for the entire value. In addition to identifying the *value type* associated with value, the offset and length of the embedded substring requiring obfuscation are supplied; use of `maskValue` with these three values and no *mask style* will apply the default obfuscation only to the embedded password. Alternatively, if a *mask style* is supplied, use of `maskValue` with this style and the original value offset and length should obfuscate only the embedded password.

> A `callback` interface is required for all calls to `getMarkupValueInfo`. The interface is only used when additional document context is required to make a determination about the requested value. In these cases, the checks are proximity-dependent and cannot be performed as part of load-time compatibility checks, when no document is available.

#### Implementations

TBD

## Load-time Compatibility

```
bool checkCompatibility(const IPTree& requirements) const;
```

### Overview

Use of runtime compatibility checks may be unavoidable in some circumstances, but reliance on this in every script is inefficient. It may also devalue trace output by omitting messaging required to debug an issue, a problem that might only be found when said messaging is needed.

The `requirements` parameter represents a standardized set of runtime compatibility checks to be applied. By default, each check must pass to establish compatibility. Explicitly optional checks may be included to detect conditions the caller is prepared to work around.

> This can test which values will or won't be affected by `maskValue`, and which obfuscation styles are available. It can test which *rule* sets will impact `maskContent`. It specifically excludes `maskMarkupValue` checks that depend on the proximity of one value to another.

Returning to the earlier SSN example, a caller might require that an SSN *value type* will be obfuscated. It might also want to use a particilar *mask style*, but may be prepared for its absence. A caller may prefer to mask the first four digits but may accept masking the last five instead, or may omit certain trace messages.

### Implementation

```
compatibility:
  - context:
      - domain: optional text
        version: optional number
        property:
          - name: required text
            value: required text
    accepts:
      - name: required text
        presence: one of "r", "o", or "p"
    uses:
      - name: required text
        presence: one of "r", "o", or "p"
    operation:
      - name: one of "maskValue", "maskContent", or "maskMarkupValue"
        presence: one of "r", "o", or "p"
    valueType
      - name: required text
        presence: one of "r", "o", or "p"
        maskStyle:
          - name: required text
            presence: one of "r", "o", or "p"
        Set:
          - name: required text
            presence: one of "r", "o", or "p"
    rule:
      - contentType: required text
        presence: one of "r", "o", or "p"
```

The `requirements` parameter of `checkCompatibility` must be either a `compatibility` element or the parent of a collection of `compatibility` elements. Each instance may contain at most one `context` element, three `operation` elements (one per operation), and a variable number of `accepts`, `uses`, `valueType`, or `rule` elements.

The `context` element describes the target of an evaluation. It may include a *domain* identifier, or assume the default *domain*. It may include a version number, or assume the default snapshot of the *domain*. It may specify any number of custom *context* properties to select various profile elements in the snapshot.

The `accepts` and `uses` elements identify custom *context* property names either accepted or used within a snapshot. Acceptance indicates some part of a snapshot has declared an understanding of the named property. Usage indicates the some part of a snapshot will react to the named value being set. Usage implies acceptance.

> To improve compatibility check capabilities, a snapshot may synthesize properties that, if set, will have no effect. Specifically, a caller may be more interested to know if a particular set name (for either *value type* or *rule* set membership) is used than to know that an unidentified set name is used.

The `Operations` element identifies which operations must be supported. Its purpose is to enforce availability of an operation when no more explicit requirements are given (for example, one may require `maskContent` without also requiring the presence of rules for any given content type). When more explicit requirements are given, the attributes of this element are redundant. If all attributes are redundant, the element may be omitted.

The `valueType` element describes all optional and mandatory requirements for using `maskValue` with a single *value type* name and optional *mask style* name. Set membership requirements can also be evaluated, which is most useful when `compatibility/context/property/@name` is "\*".

The `rule` element describes all optional and mandatory requirements for using `maskContent` with a single content type value. Unlike evaluable *value type* set membership, *rule* set membership requirements cannot be evaluated.

In all elements described as accepting `@presence`, the acceptable values are

- **r**: the check is *required* to pass
- **o**: the check is *optional*
- **p**: the check is *prohibited* from passing
