/*##############################################################################

    Copyright (C) 2022 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#pragma once

#include "jscm.hpp"
#include "jlog.hpp"
#include "jptree.hpp"
#include "tracer.h"
#include <bitset>
#include <functional>
#include <limits>

/// Bit mask indicating an operation provider supports the *mask value* operation.
static const uint8_t DataMasking_MaskValue       = 0x01;
/// Bit mask indicating an operation provider supports the *mask content* operation.
static const uint8_t DataMasking_MaskContent     = 0x02;
/// Bit mask indicating an operation provider supports the *mask markup value* operation.
static const uint8_t DataMasking_MaskMarkupValue = 0x04;

/// Reserved set name to select members of all sets. Applies to value types and rules.
static const char* DataMasking_AnySet = "*";
/// Reserved value type name to test availability of unconditional masking.
static const char* DataMasking_UnconditionalValueTypeName = "*";

/// Custom context property name to select a value type set.
static const char* DataMasking_ValueTypeSet = "valuetype-set";
/// Custom context property name to select a rule set.
static const char* DataMasking_RuleSet = "rule-set";

/**
 * @brief Iterator modeled after `IIteratorOf` but applied to unshared data, especially primitive
 *        data types.
 *
 * `IIteratorOf<C>::get()` requires instances of `C` to be objects with a `Link` method, precluding
 * it from being used with data types that either cannot or do not satisfy the requirement. This
 * interface eliminates `get()`, making the existence of `value_type_t::Link()` irrelevant.
 *
 * @tparam value_t
 */
template <typename value_t>
interface IIteratorOfScalar : extends IInterface
{
public:
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual const value_t& query() = 0;
};

interface IDataMaskingProfileContext;

/**
 * @brief Interface used by a profile to request additional context from the caller when deciding
 *        whether a specific markup value requires masking.
 *
 * A profile consumer provides an instance of this interface to a profile when requesting masking,
 * or masking information, related to markup values. The profile uses the instance to resolve
 * "external" masking dependencies, meaning when the decision to mask one value depends on another
 * value or on its location relative to other markup.
 *
 * For each value defined with external dependencies, the profile will negotiate the number of
 * ancestral elements (parent, grandparent, great-grandparent, et al) that are available for
 * dependency resolution. The profile declares a number it wants to use, and the instance responds
 * with the number actually available, if less than the number requested. Reasons why the number
 * available is less than the number requested include:
 *
 * - The value occurred at a shallower document depth than requested.
 * - The instance is incapable of either detecting or accessing the requested elements. Consider
 *   limitations arising from `IPropertyTree`'s inability to access parent nodes for illustration.
 *
 * Bound by the negotiated node limit, the profile can request two types of data:
 *
 * 1. An XPath evaluation to a text value. This is used to resolve dependencies on related data.
 *    For example, if the value of element *A* is conditionally masked depending on the value of
 *    sibling element *B*, the parent of *A* can be asked for the value of its child named *B*;
 *    the use of XPath syntax like `../B` to access *B* directly from *A* is discouraged since it
 *    is not universally supported.
 * 2. Ancestral element names can be requested. This is used to resolve "contained by" dependencies.
 *    For example, if the value of element *A* is conditionally masked depending on the name of
 *    it's parent element, the parent of *A* can be asked for its name. This relationship can be
 *    extended to as many levels as are available.
 *
 * The need to mask a value can be confirmed by the profile if the profile successfully matches
 * all parts of at least one dependency. Each value with a dependency may have multiple, discrete
 * dependencies configured - either the value of *A* is masked if the value of *B* is one of a set
 * of values, or the value of *A* is masked if the name of *A*'s parent is one of a set of values.
 *
 * The need to mask a value can be rejected by the profile if the profile fails to match at least
 * one part of all dependencies.
 *
 * The need to mask a value is ambiguous if at least one part of a dependency cannot be evaluated.
 * The absence of data with access to the complete document content may strongly suggest that
 * masking is not necessary, but may not conclusively prove it. An inability to evaluate data makes
 * a decision for or against masking nothing but a guess. Handling of the ambiguity is an exercise
 * for the profile, possibly accepting context properties to influence the decision.
 */
interface IDataMaskingDependencyCallback
{
    /**
     * @brief Negotiate the acceptable `ancestralDistance` values for query requests.
     *
     * The distance refers to the number of ancestor nodes needed to evaluate XPaths to dependent
     * values. For example, if the value of element **A** is dependent upon the value of a sibling
     * element **B**, the profile would request a distance of one and the implementation would
     * respond with a value less than one only if it is incapable of accessing a parent element.
     *
     * @param ancestralDistance [in] the profile-requested distance
     *                          [out] the distance available from the instance
     * @return true  the profile-requested distance was possible
     * @return false the profile-requested distance was not possible
     */
    virtual bool        dependencyQueryHandshake(unsigned& ancestralDistance) = 0;

    /**
     * @brief Use the requested element to evaluate an XPath as text to obtain a dependent value.
     *
     * The profile will call this method when the decision of whether to mask a value is dependent
     * upon a second value elsewhere in the document. The value of an element named *Value* might
     * require masking if *Value* has a sibling element *Name* with value *password*.
     *
     * The interface implementation owns the storage of the returned buffer. If allocation is
     * necessary to extract a value from the internal data representation, the instance must cache
     * and release it upon completion. This requirement is imposed to avoid potentially unnecessary
     * data copies while transferring data from the instance to the profile.
     *
     * @param xpath
     * @param ancestralDistance
     * @return const char* if `ancestralDistance` is not more than the negotiated limit, NULL is
     *                     acceptable and indicates the value does not exist
     */
    virtual const char* queryDependentValue(const char* xpath, unsigned ancestralDistance) = 0;

    /**
     * @brief Obtain the requested element's name.
     *
     * The profile will call this method when the decision of whether to mask a value is dependent
     * upon proximity to a named element. The value of an attribute named "value" might require
     * masking if the element name is "Password".
     *
     * The instance owns the storage of the returned buffer. If allocation is necessary to extract
     * a value from the internal data representation, the instance must cache and release it upon
     * completion. This requirement is imposed to avoid potentially unnecessary data copies while
     * transferring data from the instance to the profile.
     *
     * @param ancestralDistance
     * @return const char* must not be NULL unless `ancestralDistance` is greater than the
     *                     negotiated limit
     */
    virtual const char* queryDependentName(unsigned ancestralDistance) = 0;
};

/**
 * @brief Abstract interface describing core masking operations and their availability.
 *
 * Implementations of this interface, and its extensions, are not required to support all
 * operations. Implementations are expected to identify which they do support. Implementations
 * are also expected to fail silently in response to unsupported operation requests.
 *
 * - `maskValue` masks individual values based on an masker-defined meanings. For example, a
 *   value identified as a password might require masking regardless of the context in which it
 *   appears.
 * - `maskContent` masks a variable number of values based on context provided by surrounding text.
 *   For example, the value of an HTTP *authentication* header or the text between `<Password>` and
 *   `</Password>` might require masking. This operation can apply to both structured and
 *   unstructured text content.
 * - `maskMarkupValue` masks individual values based on their locationa within an in-memory
 *   representation of a structured document, such as XML or JSON. For example, the value of
 *   element `Password` might require masking unconditionally, while the value of element `Value`
 *   might require masking only if a sibling element named `Name` exists with value *password*.
 *   This operation relies on the caller's ability to supply context parsed from structured
 *   content.
 *
 * Each operation applies the requirements defined in one revision of a data domain. Each masker
 * implementation defines which revision is used, and this revision is referred to as the effective
 * version.
 *
 * Some implementations are stateless while others are stateful. A stateless implementation will
 * produce the same output for the same input every time. A stateful implementation allows callers
 * to affect the output by manipulating state variables. Two well known state variables are
 * *value type set* and *rule set*, both of which are described elsewhere and referenced here due
 * to their known impacts on operation results. Implementations may define additional state
 * variables which impact operation results in unknown ways.
 */
interface IDataMasker : extends IInterface
{
    /**
     * @brief Return an identifier for the data domain used in requests.
     *
     * @return const char*
     */
    virtual const char* queryDomain() const = 0;

    /**
     * @brief Identify the data domain revision used in requests.
     *
     * @return uint8_t
     */
    virtual uint8_t queryVersion() const = 0;

    /**
     * @brief Return a bit-mask representing which operations an implementation supports.
     *
     * - `DataMasking_MaskValue` is set if `maskValue` is supported.
     * - `DataMasking_MaskContent` is set if `maskContent` is supported.
     * - `DataMasking_MaskMarkupValue` is set if `maskMarkupValue` is supported.
     *
     * @return uint8_t
     */
    virtual uint8_t supportedOperations() const = 0;
    inline bool canMaskValue() const { return (supportedOperations() & DataMasking_MaskValue); }
    inline bool canMaskContent() const { return (supportedOperations() & DataMasking_MaskContent); }
    inline bool canMaskMarkupValue() const { return (supportedOperations() & DataMasking_MaskMarkupValue); }

    /**
     * @brief Apply a mask to a value when appropriate.
     *
     * A masker defines types of values, referred to in this framework as *value types*, which
     * require masking in some, if not all, situations. For each value type, it may also define
     * custom masking behaviors (e.g., masking all but four digits of an account number).
     *
     * The decision to mask is based on the values of `valueType` and `conditionally`:
     *
     * - If `valueType` is defined and not selected by the current value type set, masking is never
     *   appropriate.
     * - If `valueType` is defined and is selected by the current value type set, masking is always
     *   appropriate.
     * - If neither `valueType` nor a value type named `DataMasking_UnconditionalValueTypeName` are
     *   defined, masking is never appropriate.
     * - If `valueType` is not defined and a value type named
     *   `DataMasking_UnconditionalValueTypeName` is both defined and selected by the current value
     *   type set, masking is appropriate only when `conditionally` is *false*.
     *
     * When masking is appropriate, a mask will be applied. The mask to be applied depends on both
     * `maskStyle` and the value type which determined the need to mask:
     *
     * - If `maskStyle` is a defined style for the determining value type, it will be applied.
     * - If `maskStyle` is not defined by the determinining value type, the default style of the
     *   determining value type will be applied.
     *
     * @param valueType     Masker-defined identifier describing the nature of the value. Must not
     *                      be empty.
     * @param maskStyle     Masker-defined identifier describing the desired mask effect. May be
     *                      empty.
     * @param buffer        Character array containing the value string to be masked in place.
     * @param offset        Number of bytes from `buffer` where the value string begins.
     * @param length        Number of bytes in the value string. May be `size_t(-1)` if `buffer +
     *                      offset` is NULL terminated.
     * @param conditionally Flag controlling whether the masker must define `valueType` in order
     *                      to apply a mask (*true*) or if a mask should always apply a mask
     *                      (*false*). This flag has no effect when used with maskers that do not
     *                      define a value type named `DataMasking_UnconditionalValueTypeName`.
     * @return true         A mask has been applied to the value string.
     * @return false        The value string is unchanged.
     */
    virtual bool maskValue(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length, bool conditionally) const = 0;
    inline bool maskValueConditionally(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length) const { return maskValue(valueType, maskStyle, buffer, offset, length, true); }
    inline bool maskValueUnconditionally(const char* valueType, const char* maskStyle, char* buffer, size_t offset, size_t length) const { return maskValue(valueType, maskStyle, buffer, offset, length, false); }

    /**
     * @brief Locate and mask any number of values within a character array.
     *
     * A masker defines rules informing it how to locate values requiring masking within a block of
     * text. It applies the appropriate subset of rules to the given text to find and mask values.
     * A description of rules and the application of masking to found values are implementation
     * details.
     *
     * The subset of appropriate rules is determined by:
     *
     * - A rule not included in the effective masker version is not appropriate. Refer to specific
     *   masker extensions for more information about the effective version.
     * - A rule defined by a value type not selected by the current value type set is not
     *   appropriate.
     * - A rule not selected by the current rule set is not appropriate.
     * - A rule with a defined content type is not appropriate if `contentType` is not empty and
     *   does not match the defined value.
     * - All other rules are appropriate.
     *
     * @param contentType Masker-defined descriptor for the nature of text contained in the
     *                    character array. May be empty. Examples include *xml* or *json*.
     * @param buffer      Character array containing content to be analyzed and updated.
     * @param offset      Number of bytes from `buffer` where the content begins.
     * @param length      Number of bytes in the content. May be `size_t(-1)` if `buffer + offset`
     *                    is NULL terminated.
     * @return true       At least one value was identified and masked in the content.
     * @return false      The content is unchanged.
     */
    virtual bool maskContent(const char* contentType, char* buffer, size_t offset, size_t length) const = 0;

    /**
     * @brief Apply a mask to a value based on its position within a structured document, e.g., a
     *        document that can be represented internally as an `IPropertyTree`. Such a
     *        representation is not a requirement, and is mentioned solely to present a concept
     *        using well known ideas and terminology.
     *
     * A masker defines rules informing it which values in a structured document require masking.
     * The choice of which rules to use for a request is the same as for `maskContent`. A rule may
     * match the `element` and `attribute` input either conditionally or unconditionally.
     *
     * An unconditional match suggests that the input parameters are sufficient to require masking.
     * The example of an element named *Password* is an unconditional match. The value of every
     * element with this name requires masking.
     *
     * A conditional match implies that the value may require masking, but additional information
     * is required to make a final determination. In this case, `callback` will be used to request
     * the required information. Masking is only required if `callback` is able to provide the
     * requested information and that information matches the rule's conditions.
     *
     * Details of the application of masking when required is an implementation detail
     *
     * @param element   A value like a property tree element name. This may be empty if `attribute`
     *                  is not empty.
     * @param attribute A value like a property tree attribute name. This may be empty if the value
     *                  would be property tree element content and not property tree attribute
     *                  content.
     * @param buffer    Character array containing a value string to be masked.
     * @param offset    Number of bytes from `buffer` where the value string begins.
     * @param length    Number of bytes in the value string. May be `size_t(-1)` if `buffer +
     *                  offset` is NULL terminated.
     * @param callback  Instance of `IDataMaskingDependencyCallback` used to request conditionally
     *                  required context. All details of the structured document's internal
     *                  representation are hidden from the masker by this parameter.
     * @return true     A mask was applied to the value.
     * @return false    The value is unchanged.
     */
    virtual bool maskMarkupValue(const char* element, const char* attribute, char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback) const = 0;
};

/**
 * @brief Describes the nature of values to be masked, along with the rules used to identify
 * these values in structured content.
 *
 * A profile associates collections of value types and rules with one or more profiles identifiers.
 * The use of multiple identifiers allows a single profile to support multiple revisions of its
 * description.
 *
 * Consider a profile defined to mask data in the US market. The implementation might accept the
 * identifier `urn:hppc:mask:us` as a request for the lastest definition, and either
 * `urn:hopcc::mask:us:1` or `urn:hpcc::mask:us:2` for a specific revision. An ESDL ESP might
 * request a profile based on the version-agnostic value, but forward the versioned identifier
 * to (possibly) remote log agents so they may apply the exact same rules.
 */
interface IDataMaskingProfile : extends IDataMasker
{
    /**
     * @brief Create a stateful context referencing a specific domain revision.
     *
     * Context creation accepts an optional trace output handler. This enables a caller to override
     * the the profile's default handler if needed. For example, an ESP might create a default
     * handler at service start and allow individual transactions to override default behavior.
     *
     * @param version The requested data domain revision. May be zero to request the default.
     * @param tracer  An `ITracer` instance to be used for profile trace output. May be NULL.
     * @return IDataMaskingProfileContext*
     */
    virtual IDataMaskingProfileContext* createContext(uint8_t version, ITracer* tracer) const = 0;

    /**
     * @brief Return a reference to a profile inspection implementation.
     *
     * The implementation of this interface may also implement the inspector interface. This should
     * not be assumed.
     *
     * @return const IDataMaskingProfileInspector&
     */
    virtual const interface IDataMaskingProfileInspector& inspector() const = 0;
};

/**
 * @brief Extension of `IIteratorOf<IDataMaskingProfile>`.
 *
 * A plugin entry point function returns an instance of this when initializing an engine. This use
 * case does not call for a scalar iterator, so the standard is used.
 */
interface IDataMaskingProfileIterator : extends IIteratorOf<IDataMaskingProfile> {};

/**
 * @brief Shared library entry point function.
 *
 * A plugin acts on a property tree node to produce an iterable collection of profiles. The node,
 * provided by an engine implemeentation,
 */
typedef IDataMaskingProfileIterator* (*DataMaskingPluginEntryPoint)(const IPTree&, ITracer&);

/**
 * @brief Abstract representation of consumer-specific contextual data.
 *
 * Designates which profile version to be used. Manages custom properties.
 *
 * Contexts, created by profiles, retain knowledge of the profiles used to create them. With this
 * information, a context may be used in place of a profile as an operation provider.
 */
interface IDataMaskingProfileContext : extends IDataMasker
{
    /**
     * @brief Return the optional tracer used with requests.
     *
     * @return ITracer* The optional trace output handler supplied during creation. May be NULL.
     */
    virtual ITracer* queryTracer() const = 0;

    /**
     * @brief Indicate whether any custom properties are defined.
     *
     * @return true  At least one property has been set.
     * @return false No properties have been set.
     */
    virtual bool        hasProperties() const = 0;

    /**
     * @brief Indicate whether a named custom property is defined.
     *
     * @param name   A unique property idenntifier. Should not be empty.
     * @return true  The indicated property has been set.
     * @return false The indicated property has not been set.
     */
    virtual bool        hasProperty(const char* name) const = 0;

    /**
     * @brief Obtain a named custom property's value, or NULL if not defined.
     *
     * @param name         A unique property identifier. Should not be NULL.
     * @return const char* The value of the indicated property. May be NULL when `name` is invalid
     *                     or has not been set.
     */
    virtual const char* queryProperty(const char* name) const = 0;

    /**
     * @brief Define a named custom property.
     *
     * The return value is an implementation detail, but should reflect whether the property is
     * "acceptable" to the profile. The implementation must choose whether it is sufficient for
     * the profile to merely *accept* a property, or if the profile must *use* the property. The
     * default implementation does not require property usage.
     *
     * @param name   A unique property identifier. Should not be empty.
     * @param value  The content associated with the identified property. Should not be NULL.
     * @return true  The given content is associated with the identified property.
     * @return false The given content could not be associated with the identified property.
     */
    virtual bool        setProperty(const char* name, const char* value) = 0;

    /**
     * @brief Remove a definition of a named custom property.
     *
     * @param name   A unique property identifier. Should not be empty.
     * @return true  The indicated property was and is no longer set.
     * @return false The indicated property was not set.
     */
    virtual bool        removeProperty(const char* name) = 0;

    /**
     * @brief Create a copy of an instance.
     *
     * A consumer which has configured a context with various custom properties may want to support
     * temporarily changing and restoring its state. Using clones enables a *context stack* to be
     * implemented by consumers as needed. This framework does not provide such a stack.
     *
     * @return IDataMaskingProfileContext*
     */
    virtual IDataMaskingProfileContext* clone() const = 0;

    /**
     * @brief Return a reference to a profile context inspection implementation.
     *
     * The implementation of this interface may also implement the inspector interface. This should
     * not be assumed.
     *
     * @return const IDataMaskingProfileContextInspector&
     */
    virtual const interface IDataMaskingProfileContextInspector& inspector() const = 0;
};

/**
 * @brief Abstraction representing one or more domains defined by one or more shared libraries.
 *
 * An engine emplementation manages a collection of domains, where each domain is a collection of
 * closely related profiles. The interface is geared towards using profiles loaded from shared
 * libraries, but this is not a requirement for each implementation.
 *
 * There are three ways of using the engine once configured:
 *
 * 1. A profile context can be requested for a domain and version. Domain may be omitted when
 *    only one domain is loaded. Version may be omitted to request the default version of the
 *    domain's default profile. The default profile is determined by the engine implementation.
 * 2. A profile can be requested for a domain and version. Domain may be omitted when only one
 *    domain is loaded. Version may be omitted to request the domain's default profile.
 * 3. When only one domain is loaded, an instance can be used as a shortcut to the default
 *    version of the domain's default profile.
 *
 * Implementations must not require use as a singleton. ESDL service integration may allow
 * each binding to define its own masking requirements, leading to two or more engines loaded in
 * one process.
 */
interface IDataMaskingEngine : extends IDataMasker
{
    /**
     * @brief Load masking profiles based on the contents of `configuration`.
     *
     * @param configuration
     * @return size_t the number of profiles successfully loaded, or size_t(-1) on error
     */
    virtual size_t loadProfiles(const IPTree& configuration) = 0;

    /**
     * @brief Obtain a profile for the requested domain that supports the indicated version.
     *
     * A non-NULL response indicates that the requested version of the requested domain exists and
     * is supported by the profile. If the requested version is not the profile's default version,
     * it is necessary to request a context from this profile for the desired version.
     *
     * @param domainId
     * @param version
     * @return IDataMaskingProfile*
     */
    virtual IDataMaskingProfile* queryProfile(const char* domainId, uint8_t version) const = 0;
    inline IDataMaskingProfile* getProfile(const char* domainId, uint8_t version) const { return LINK(queryProfile(domainId, version)); }

    /**
     * @brief Obtain a profile context for the requested domain and version.
     *
     * A non-NULL response indicates that the requested version of the requested domain is available
     * for use.
     *
     * @param domainId
     * @param version
     * @param tracer   forwarded to the context constructor
     * @return IDataMaskingProfileContext*
     */
    virtual IDataMaskingProfileContext* getContext(const char* domainId, uint8_t version, ITracer* tracer) const = 0;

    /**
     * @brief Return a reference to a profile context inspection implementation.
     *
     * The implementation of this interface may also implement the inspector interface. This should
     * not be assumed.
     *
     * @return const IDataMaskingEngineInspector&
     */
    virtual const interface IDataMaskingEngineInspector& inspector() const = 0;
};

/// Name used for a scalar iterator of character pointers.
using ITextIterator = IIteratorOfScalar<const char*>;

/**
 * @brief Declaration of data and behaviors shared by all profile components.
 *
 * All profile components include:
 *
 * - A name, which may be optional or required. If required for a component type, the value must
 *   be unique among all instances of a component type applicaple to a version.
 * - A non-empty range of versions to which the instance applies.
 *
 * All profile components can:
 *
 * - Evaluate compatibility with a profile context. This includes, at a minimum, a version check.
 *
 * To illustrate, consider an instance named *foo* applied to versions 1 and 2. A second instance
 * named *foo* is allowed if its version range includes neither 1 nor 2. A second instance named
 * *foo* that includes either version 1 or 2 is an error that invalidates both instances.
 */
interface IDataMaskingProfileEntity : extends IInterface
{
    virtual const char* queryName() const = 0;
    virtual uint8_t     queryMinimumVersion() const = 0;
    virtual uint8_t     queryMaximumVersion() const = 0;
    virtual bool        matches(const IDataMaskingProfileContext* context) const = 0;
};

/**
 * @brief A profile component representing a custom mask style.
 *
 * All mask styles can:
 *
 * - Apply themselves to a designated range of text. How masking is applied is a primarily function
 *   of its initial configuration, but may adapt to custom context properties.
 */
interface IDataMaskingProfileMaskStyle : extends IDataMaskingProfileEntity
{
    virtual bool applyMask(const IDataMaskingProfileContext* context, char* buffer, size_t offset, size_t length) const = 0;
};

/// Name used for a scalar iterator of mask styles.
using IDataMaskingProfileMaskStyleIterator = IIteratorOfScalar<IDataMaskingProfileMaskStyle>;

/**
 * @brief A profile component representing a value type.
 *
 * All value types can:
 *
 * - Evaluate set membership. Set membership may be used to limit which instances match a context.
 * - Interrogate available custom mask styles. Implementations are encouraged to provide a default
 *   mask style that cannot be interrogated, but may support using custom styles as the default.
 */
interface IDataMaskingProfileValueType : extends IDataMaskingProfileEntity
{
    /**
     * @brief Determines whether the instance belongs to a given user-defined set of types.
     *
     * @param set
     * @return true
     * @return false
     */
    virtual bool isMemberOf(const char* set) const = 0;

    /**
     * @brief Lookup a custom mask style by name.
     *
     * @param name
     * @param context
     * @return IDataMaskingProfileMaskStyle*
     */
    virtual IDataMaskingProfileMaskStyle* queryMaskStyle(const IDataMaskingProfileContext* context, const char* name) const = 0;
    inline IDataMaskingProfileMaskStyle* getMaskStyle(const IDataMaskingProfileContext* context, const char* name) const { return LINK(queryMaskStyle(context, name)); }

    /**
     * @brief Get an iterator of all custom mask styles defined for the instance.
     *
     * @param context
     * @return IDataMaskingProfileMaskStyleIterator*
     */
    virtual IDataMaskingProfileMaskStyleIterator* getMaskStyles(const IDataMaskingProfileContext* context) const = 0;

    /**
     * @brief Apply a mask style to the indicated character range.
     *
     * Implementations should assume that masking is required when invoked. With this assumption,
     * masking should always be applied to the indicated text. If the requested mask style is
     * invalid (e.g., it does not exist or does not apply to the context), a default mask should
     * be applied.
     *
     * @param maskStyle
     * @param buffer
     * @param offset
     * @param length
     * @param context
     * @return true
     * @return false
     */
    virtual bool applyMask(const IDataMaskingProfileContext* context, const char* maskStyle, char* buffer, size_t offset, size_t length) const = 0;
};

/// Name used for a scalar iterator of value types.
using IDataMaskingProfileValueTypeIterator = IIteratorOfScalar<IDataMaskingProfileValueType>;

/**
 * @brief Describes how to mask a markup value using the *mask value* operation.
 *
 * Specify which, if any, characters require masking given a combination of the `buffer`, `offset`,
 * and `length` *mask value* paremeters. Masking is required if `valueType` is not NULL. If masking
 * is required:
 *
 * - `maskOffset` will not be less than `offset`
 * - `maskOffset` will be less than `offset + length`
 * - `maskLength` will not be more than `length`
 * - `maskOffset + maskLength` will not be more than `offset + length`
 * - `valueType` will identify the nature of the given value
 * - `maskStyle` will identify the mask style used to calculate both `maskOffset` and `maskLength`
 * - Use `buffer`, `maskOffset`, `maskLength`, and `valueType` (with the default mask style) to mask
 *   the required character range.
 * - Use `buffer`, `offset`, `length`, and `maskStyle` to mask the required character range,
 *   recalculating `maskOffset` and `maskLength` in the process.
 *
 * Consider an account number for which all but the last four digits require masking. A request for
 * markup value info will identify the given value's type and the value substring to be masked. It
 * may also identify a non-default mask style used to calculate the substring. Applying a mask
 * using `valueType` and the default mask style touches only the characters to be masked. Because
 * `maskStyle` identified the value substring to be masked, asking it to mask the value requires it
 * to repeat the substring identification process; supplying only the substring to the style would
 * result in masking all but the last eight digits.
 */
struct DataMaskingMarkupValueInfo
{
    size_t maskOffset = 0;
    size_t maskLength = 0;
    IDataMaskingProfileValueType* valueType = nullptr;
    IDataMaskingProfileMaskStyle* maskStyle = nullptr;
};

/**
 * @brief Interface for profile content interrogation.
 *
 * Instances of this interface are returned by `IDataMasker` implementations. The interface
 * enables a consumer to identify a masker and many of its capabilities.
 *
 * - For `maskValue`, available value types can be queried. Available mask styles for each value
 *   type are available from the returned value type instance.
 * - For `maskContent`, the existence of rules matching a content type can be queried. Individual
 *   rules cannot be queried since a masker cannot individually apply a rule.
 * - For `maskMarkupValue`, a value can be identified as requiring masking or not.
 * - For any operation, supported custom properties can be queried.
 */
interface IDataMaskerInspector : extends IInterface
{
    virtual bool isValid() const = 0;
    virtual const char* queryDefaultDomain() const = 0;
    virtual bool acceptsDomain(const char* id) const = 0;
    virtual ITextIterator* getAcceptedDomains() const = 0;
    virtual uint8_t queryMinimumVersion() const = 0;
    virtual uint8_t queryMaximumVersion() const = 0;
    virtual uint8_t queryDefaultVersion() const = 0;
    virtual IDataMaskingProfileValueType* queryValueType(const char* name) const = 0;
    virtual bool hasValueType() const = 0;
    virtual size_t countOfValueTypes() const = 0;
    virtual IDataMaskingProfileValueTypeIterator* getValueTypes() const = 0;
    virtual bool acceptsProperty(const char* name) const = 0;
    virtual ITextIterator* getAcceptedProperties() const = 0;
    virtual bool usesProperty(const char* name) const = 0;
    virtual ITextIterator* getUsedProperties() const = 0;
    virtual bool hasRule(const char* contentType) const = 0;
    virtual size_t countOfRules(const char* contentType) const = 0;
    virtual bool getMarkupValueInfo(const char* element, const char* attribute, const char* buffer, size_t offset, size_t length, IDataMaskingDependencyCallback& callback, DataMaskingMarkupValueInfo& info) const = 0;

    inline IDataMaskingProfileValueType* getValueType(const char* name) const { return LINK(queryValueType(name)); }
};

/**
 * @brief Extension if `IDataMaskerInspector` representing a profile's masker inspector.
 */
interface IDataMaskingProfileInspector : extends IDataMaskerInspector
{
};

/**
 * @brief Abstract representation of a context's property enabling iteration of all of a context's
 *        properties.
 *
 * Context implementations are not required to derive their property storage from this interface.
 * This interface may be treated as a logical overlay on top of storage optimized for access. For
 * example, a map of name to value might be used to store properties with this abstraction wrapping
 * the map's internal storage.
 *
 * It does not extend `IInterface`. Iteration requires use of a scalar iterator.
 */
interface IDataMaskingContextProperty
{
    /**
     * @brief Return the name of a custom property.
     *
     * @return const char*
     */
    virtual const char* queryName() const = 0;

    /**
     * @brief Return the value if a custom property.
     *
     * @return const char*
     */
    virtual const char* queryValue() const = 0;
};

/// Name used for a scalar iterator of `IDataMaskingContextProperty`.
using IDataMaskingContextPropertyIterator = IIteratorOfScalar<IDataMaskingContextProperty>;

/**
 * @brief Extension of `IDataMaskerInspector` representing a profile context's masker inspector.
 */
interface IDataMaskingProfileContextInspector : extends IDataMaskerInspector
{
    /**
     * @brief Get a new iterator of all defined custom properties.
     *
     * @return IDataMaskingContextPropertyIterator*
     */
    virtual IDataMaskingContextPropertyIterator* getProperties() const = 0;

    /**
     * @brief Return the profile that created this instance.
     *
     * A reference is returned instead of a pointer. The implementation must retain a reference
     * to the profile that creates it to satisfy this request.
     *
     * @return const IDataMaskingProfile&
     */
    virtual const IDataMaskingProfile& queryProfile() const = 0;
};

/**
 * @brief Name used for an STL `bitset` for which each bit index represents a version number.
 *
 * Bit zero is unused as it is not a valid version number. Remaining bits are set to *true*
 * when the corresponding version is supported and *false* when it is unsupported.
 */
using DataMaskingVersionCoverage = std::bitset<(std::numeric_limits<uint8_t>::max() + 1)>;

/**
 * @brief Extension of `IDataMaskerInspector` representing an engine's masker inspector.
 *
 * The masker described by the inherited `IDataMaskerInspector` interface is assumed to be the
 * default profile of the default domain, i.e., the profile used by the engine for masking
 * operations.
 *
 * In addition to describing the default profile, the engine's state with respect to domains may
 * be queried.
 */
interface IDataMaskingEngineInspector : extends IDataMaskerInspector
{
    /**
     * @brief Return the number of loaded domains.
     *
     * @return size_t
     */
    virtual size_t domainCount() const = 0;

    /**
     * @brief Determine if at least one profile for the domain `id` is being managed.
     *
     * @param id
     * @return true
     * @return false
     */
    virtual bool hasDomain(const char* id) const = 0;

    /**
     * @brief Get an iterator of all default domain identifiers.
     *
     * Every domain supports a number of acceptable identifiers. One of these is considered the
     * default for that domain. The iterator provides access to all such default identifiers.
     * @return ITextIterator*
     */
    virtual ITextIterator* getDomains() const = 0;

    /**
     * @brief Get an iterator of all acceptable identifiers for a domain.
     *
     * @param domain
     * @return ITextIterator*
     */
    virtual ITextIterator* getDomainIds(const char* domain) const = 0;

    /**
     * @brief Populate `coverage` with the versions of domain `domainId` currently supported.
     *
     * @param domainId
     * @param coverage
     */
    virtual void getDomainVersions(const char* domainId, DataMaskingVersionCoverage& coverage) const = 0;
};
