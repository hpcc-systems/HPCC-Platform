## libdatamasker.so

This implements a plugin library producing instances of `IDataMaskingProfileIterator` to be used by an `IDataMaskingEngine` instance. The library is written in C++, and this section is organized according to the namespace and class names used.

### namespace DataMasking

#### CContext

Standard implementation of `IDataMaskingProfileContext` and `IDataMaskingProfileContextInspector` tightly coupled to a specific version of a specific profile instance. It manages custom properties that are accepted by the associated profile, and rejects attempts to set those not accepted. The rejection is one way of letting the caller know that something it might deem essential is not handled by the profile.

Depends on `CProfile`, an abstract implementation of `IDataMaskingProfile` and `IDataMaskingProfileInspector`.

#### CMaskStyle

Implementation of `IDataMaskingProfileMaskStyle` providing customized masked output of values. Configuration options include:

- **@maximumVersion** is an optional version number, needed only when the style does not apply to the maximum version of the value type in which it is defined.
- **@minimumVersion** is an optional version number, needed only when the style does not apply to the minumum version of the value type in which it is defined.
- **@name** is a required unique identifier for the style.
- **@overrideDefault** is an optional Boolean flag controlling whether the style should replace the default style for the value type in which it is defined.
- **@pattern** is an optional character sequence (one or more characters in length) that will be used to mask value content.

#### CPartialMaskStyle

Am extension of [CMaskStyle](#cmaskstyle), this is intended to partially mask values, such as account numbers or telephone numbers, without assuming knowledge of the values. Configuration is generally expressed as:

    [ [ @action ] [ @location ] @count  [ @characters ] ]

Where:

- **@action** what to do, either *keep* or *mask* (default); and
- **@location** where to do it, either *first* or *last* (default); and
- **@count** how many characters to examine, a positive integer value; and
- **@characters** type of characters to be considered, either *numbers*, *letters*, *alphanumeric*, or *all* (default)

All four values may be omitted when only the inherited options are needed. If any of the four values are given, **count** is required and the other three are optional.

A value substring containing at most **@count** instances of the character class denoted by **@characters** is identified. Character classes are ASCII numeric characters (*numbers*), ASCII alphabetic characters (*letters*), ASCII alphabetic and numeric characters (*alphanumeric*), or any characters (*all*).

The value substring is at the start of the value when **@location** is *first*, and at the end of the value when **@location** is *last*.

The value substring is masked when **@action** is *mask*. All of the value with the exception of the substring is masked when **@action** is *keep*.

#### CRule

A base class for rule implementations, this defines standard rule properties without providing any content knowledge for use with `maskContent`.

Configuration includes:

- **@contentType** is an optional, user-defined, label describing the content format to which the rule should be applied. `maskContent` requests that indicate a content type should apply all rules that match the type in addition to all rules that omit a type.
- **memberOf** is an optional and repeating element identifying user-defined set labels. Only rules with membership in the requested set are considered; in the absence of an explictly requested set, a default set with an empty name is implied.
- **memberOf/@name** is a required user-defined set label.

rule instances will frequently be specific to a content format. For example, a rule applied to XML markup value may include dependencies on XML syntax which are not applicable when masking JSON content. Rules specific to a markup format can be associated with that format using **@contentType**. Requests to mask content of a type will apply all rules without a **@contentType** value or with a matching value, and will exclude all rules with a non-matching value.

#### CSerialTokenRule

An extension of [CRule](#crule), this identifies content substrings to be masked based on matching start and end tokens in the content buffer. For each occurrence of a configured start token that is balanced by a corresponding configured end token, the characters between the tokens are masked.

This class may be used with [TSerialProfile](#tserialprofile).

Configuration includes:

- **@endToken** is an optional character sequence expected to immediately follow a content substring to be masked. This might be an XML element end tag, or a terminating double quote for a JSON value. A newline, i.e., *\\n*, is assumed if omitted or empty, for masking values such as HTTP headers.
- **@matchCase** is an optional Boolean flag controlling whether token matches are case sensitive (*true*) or insensitivie (*false*). Matches are case insensitive by default. The implementation is based on ASCII data; case sensitive comparisons of similarly encoded non-ASCI text can work, but case insensitive comparisons are not supported.
- **@startToken** is a required character sequence expected to precede a content substring to be masked.

> No content type knowledge is implied by this class. An instance with **@contentType** of *xml* does not inherently know how to find values in XML markup. The defined tokens must include characters such as `<` and `>` to match an element name and quotes to match attribute values.

#### TPlugin

An implementation of `IDataMaskingProfileIterator` used for transforming a configuration property tree into a collection of profiles to be returned by a library entry point function. Created profiles are of the same class, identified by the template parameter **profile_t**.

Configuration includes:

- **profile** An optional and repeating element defining a profile. The content of this element depends on **profile_t**.
- If and only if **profile** is not included as a child of the configuration node, the node itself will be treated as a configuration for **profile_t**. The name of the configuration node is not required to be **profile**.

#### TProfile

A concrete extension of `CProfile`, this manages value types and rules, providing a default implementation of `maskValue` but leaving other operations to subclasses.

Template parameters are:

- **valuetype_t** identifies the concrete implementation of `IDataMaskingProfileValueType` to be instantiated during configuration.
- **rule_t** identifies the concrete implementation of rules to be managed. Creation is assumed to be the responsibility of the value type.
- **context_t** identifies the concrete implementation of `IDataMaskingProfileContext` to be instantiated on demand.

Configuration includes:

- **@defaultVersion** is an optional version number identifying the version to be used when version *0* is requested. Omission or *0* implies the maximum version (which is configured before this value).
- **@domain** is the required default identifier used to select this profile.
- **legacyDomain** is an optional and repeating element identifying alternate identifiers by which the profile may be selected. This enables domain identifier naming conventions to be changed without breaking pre-existing references.
- **legacyDomain/@id** is a required identifier of this profile.
- **@maximumVersion** is an optional version number identifying the highest version supported by the configuration. Omission or *0* implies a value matching the minimum version (whether implicitly or explicitly defined).
- **@minimumVersion** is an optional version number identifying the lowest version supported by the configuration. Omission or *0* implies 1.
- **@name** is an optional label for the instance. If given, the value is used in trace output.
- **property** is an optional and repeating element describing a custom context property recognized by the profile. The profile can declare awareness of properties without explicit use of them.
- **property/@name** is a required context property name.
- **property/@minimumVersion** is an optional version number indicating the lowest profile version aware of the property. Omission or *0* implies the profile minimum.
- **property/@maximumVersion** is an optional version number indicating the highest profile version aware of the property. Omisssion or *0* implies the profile maximum.
- **valueType** is an optional and repeating element describing the value types defined by the profile. Element content depends on the value of **valuetype_t**.

For profiles supporting a single version, value type names must be unique. For profiles supporting multiple versions, value type names may be repeated but must be unique for each version. To illustrate, consider this snippet:

    profile:
      minimumVersion: 1
      maximumVersion: 2
      valueType:
        - name: foo
          maximumVersion: 1
        - name: foo
          minimumVersion: 2
        - name: bar
        - name: bar
          minimumVersion: 2

In the example, *foo* and *bar* are each defined twice. The redefinition of *foo* is acceptable because each instance applies to a different version. The redefinition of *bar* is invalid because both instances claim to apply to version *2*.

The value type name *\** is reserved by this class. A profile that includes a value type named *\** supports unconditional value masking. `maskValue` requests specifying unknown value type names can fall back to this special type and force masking for these values. Without this type, `maskValue` masks what it thinks should be masked; with this type, it trusts the caller to request masking for only those values known to require it. Value types included in unselected value type sets are known to the profile and, as such, can be used to prevent specific typed values from ever being masked.

> The requirement of a type definition instead of a simpler flag is to enable the definition of mask styles. It has the side effect of enabling the definition of rules. In theory, an entire profile could be defined using a single value type. This may make sense in some cases, but not in all. For example, a partial mask style intended for use with U.S. Social Security numbers could be inappropriately applied to a password. Use care when configuring this type.

#### TSerialProfile

An extension of [TProfile](#tprofile) that adds support for `maskContent`. It is assumed by `maskContent` that each applicable rule must be applied serially, i.e., one after the other, using an `bool applyRule(buffer, length, context)` interface.

Template parameters are unchanged from `TProfile`.

Configuration options are unchanged from `TProfile`.

#### TValueType

An implementation of `IDataMaskingProfileValueType` that manages mask styles and creates rules for the profile.

Template parameters are:

- **maskstyle_t** identifies the concrete implementation of `IDataMaskingProfileMaskStyle` to be instantiated during configuration.
- **rule_t** identifies the concrete implementation of rules to be created during configuration.

Configuration includes:

- **@maximumVersion** is an optional version number, needed only when the type does not apply to the maximum version of the profile in which it is defined.
- **@minimumVersion** is an optional version number, needed only when the type does not apply to the minumum version of the profile in which it is defined.
- **maskStyle** is an optional and repeating element describing mask styles defined by the type. Element content depends on the value of **maskstyle_t**.
- **memberOf** is an optional and repeating element identifying user-defined set labels. All value types that are not explicitly assigned set membership are considered for all masking requests. Value types assigned set membership are considered only when one of their assigned sets is selected with the request context.
- **memberOf/@name** is a required user-defined set label.
- **@name** is a required unique identifier for the type.
- **rule** is an optional and repeating element describing rules defined by the type. Element content depends on the value of **rule_t**.

For value types supporting a single version, mask style names must be unique. For types supporting multiple versions, style names may be repeated but must be unique for each version. To illustrate, consider this snippet:

    valueType:
      minimumVersion: 1
      maximumVersion: 2
      maskStyle:
        - name: foo
          maximumVersion: 1
        - name: foo
          minimumVersion: 2
        - name: bar
        - name: bar
          minimumVersion: 2

In the example, *foo* and *bar* are each defined twice. The redefinition of *foo* is acceptable because each instance applies to a different version. The redefinition of *bar* is invalid because both instances claim to apply to version *2*.

### Entry Point Functions

This section describes the entry point functions exported by the shared library. The library must export one function, and may export multiple functions.

The description of each entry point will identify which of the previously described classes is used to represent the returned collection of profiles. If a templated class is identified, the template parameters are also listed. Refer to the class descriptions for additional information.

#### newPartialMaskSerialToken

Returns a (possibly empty) collection of profiles supporting `maskValue` and `maskContent` operations.

- The profile collection is an instance of [TPlugin](#tplugin).
- Collection profiles are implemented using [TProfile](#tprofile).
- Profile value types are implemented using [TValueType](#tvaluetype).
- Value type mask styles are implemented using [CPartialMaskStyle](#cpartialmaskstyle).
- Value type and profile rules are implemented using [CSerialTokenRule](#cserialtokenrule).
- Profile contexts are implemented using [CContext](#ccontext).
