/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef FXPP_FRAGMENTED_XML_FULL_PARSER_H_
#define FXPP_FRAGMENTED_XML_FULL_PARSER_H_

#include "xpp/XmlPullParser.h"

using namespace xpp;

namespace fxpp
{

/**
 * A collection of interfaces and classes used to parse an XML fragment. A fragment, in this
 * context, may be a simple, well-formed document that could be parsed by `xpp::XmlPullParser`,
 * or it may be more complex:
 *
 * - A fragment may contain a reference to externally defined markup. Such a reference may be
 *   detected by the parser and the referenced content is seamlessly injected into the sequence
 *   of events returned by the parser. The external fragment controls whether it replaces the
 *   referencing element or descends from it.
 * - A fragment may contain embedded markup, which is either CDATA-enclosed or XML-encoded XML
 *   content that a traditional parser would treat as text. Such embedded markup may be detected
 *   by the parser and the content is seamlessly injected into the sequence of events returned
 *   by the parser. The embedded fragment controls whether it replaces the parent of the embedded
 *   text or remains descended from that element.
 * - A parser event consumer may define locations within a fragment where external content should
 *   be injected into the parser's sequence of events. The parser can be compelled to parse such
 *   content on demand. The caller cannot control how existing content is handled.
 * - A fragment may be a sequence of XML elements with no root element.
 *
 * `xpp::IXmlPullParser` declares the entire public interface of `xpp::XmlPullParser`. It is also
 * the base of `fxpp::IFragmentedXmlPullparser`. Migration of existing code to use the framework
 * requires replacing references to `xpp::XmlPullParser` with the appropriate interface,
 * `xpp::IXmlPullParser` when framework extensions are not required and
 * `fxpp::IFragmentedXmlPullParser` when framework extensions are required. Additional changes may
 * be necessary, but such details depend upon individual implementations of the interface.
 *
 * To parse a single fragment, as is currently done using `xpp::XmlPullParser`, an instance of
 * `fxpp::IFragmentedXmlPullParser`, which extends `xpp::IXmlPullParser`, supports parsing similar
 * to `xpp::XmlPullParser`. Each implementation is free to deviate from the expected behavior, and
 * the default implementation prohibits the use of mixed content.
 *
 * To parse multiple fragments with manual control of additional fragments, an instance of
 * `fxpp::IFragmentedXmlPullParser` will accept new fragments with the `injectFragment` method.
 * This method cannot be called before the first `START_TAG` is reported, nor can it be called
 * after the balancing `END_TAG` is reported.
 *
 * To parse multiple fragments with automatic detection of external fragment references, an instance
 * of `fxpp::IFragmentedXmlAssistant` must be registered with the `fxpp::IFragmentedXmlPullParser`
 * instance. The assistant implementation is responsible for recognizing which markup elements may
 * specify external fragment references, which of those that may specify a reference actually do
 * specify external fragment references, loading the referenced markup, and passing it back to the
 * parser.
 * 
 * To parse multiple fragments with automatic detection of embedded fragment content, an instance
 * of `fxpp::IFragmentedXmlAssistant` must be registered with the `fxpp::IFragmentedXmlPullParser`
 * instance. The assistant implementation is responsible for recognizing which markup elements may
 * contain embedded fragment content, which of those that may contain a fragment actually do contain
 * embedded fragment content, and passing it back to the parser as a new fragment.
 * 
 * A default implementation of `fxpp::IFragmentedXmlAssistant` is provided which includes
 * handling of both automatic scenarios. Refer to `fxpp::fxa::IAssistant` for more details.
 */

/**
 * Named type for externally set parser rules related to fragment injection.
 */
using XmlFragmentRules = uint8_t;

/**
 * Abstraction providing indirect access to a parser to inject content. A parser will provide an
 * instance of this interface to its assistant to facilitate acceptance of fragment content from
 * the assistant.
 */
interface IFragmentInjector : extends IInterface
{
    /**
     * Receive fragment data from a caller. Action applied varies by implementation.
     *
     * @param uid Unique identifier associated with the fragment markup. May be NULL.
     * @param content XML markup to be parsed next. Must not be NULL.
     * @param contentLength Actual length of `content` buffer or `-1` if content is NULL terminated.
     * @param rules Instructions for integrating new markup within existing markup.
     */
    virtual void injectFragment(const char* uid, const char* content, int contentLength, XmlFragmentRules rules) = 0;
};

/**
 * Abstraction providing external and embedded fragment recognition to an IFragmentedXmlPullParser
 * instance. The parser cannot detect fragments without an assistant.
 *
 * Implementations should prepare and inject fragment content through the `IFragmentInjector`
 * instance provided to each request. Implementations should not attempt to inject content
 * directly into a parser instance in response to these requests. 
 */
interface IFragmentedXmlAssistant : extends IInterface
{
    virtual bool resolveExternalFragment(const StartTag& stag, IFragmentInjector& injector) const = 0;
    virtual bool resolveEmbeddedFragment(const StartTag& stag, const char* content, IFragmentInjector& injector) const = 0;
};

/**
 * Abstraction of a pull parser capable of recognizing and parsing externally referenced and
 * embedded XML fragments as if they are part of the initial input document being parsed.
 *
 * Fragments may be detected by a parser implementation or its assigned IFragmentedXmlAssistamt
 * instance. In the event that this combination does not detect all fragments, parser consumers
 * may forcibly inject fragments into the parser. If such injection is not needed, an initialized
 * parser may be used interchangeably with any IXmlPullParser implementation (including the
 * original XmlPullParser).
 */
interface IFragmentedXmlPullParser : extends IXmlPullParser
{
    /**
     * Manual injection of data between the first START_TAG and final END_TAG reported from the
     * parser's base input. When parsing without a registered assistant, this is the only option
     * for expanding upon the parser's base input. When parsing with a registered assistant, this
     * option should rarely be necessary.
     *
     * @param uid Unique identifier associated with the fragment markup. May be NULL.
     * @param content XML markup to be parsed next. Must not be NULL.
     * @param contentLength Actual length of `content` buffer or `-1` if content is NULL terminated.
     * @param rules Instructions for integrating new markup within existing markup.
     */
    virtual void injectFragment(const char* uid, const char* content, int contentLength, XmlFragmentRules rules) = 0;

    /**
     * Manual injection of data between the first START_TAG and final END_TAG reported from the
     * parser's base input. When parsing without a registered assistant, this is the only option
     * for expanding upon the parser's base input. When parsing with a registered assistant, this
     * option should rarely be necessary.
     *
     * Combines `prefix`, `body`, and `suffix` into a single XML snippet. No single parameter is
     * required to be well-formed, but the conmbination of all three parameters must be well-
     * formed. Generally, `prefix` and `suffix` will be used to define start and end tags for
     * elements that make `body` well-formed or that propagate namespaces from the current fragment
     * into the new fragment.
     *
     * @param uid Unique identifier associated with the fragment markup. May be NULL.
     * @param prefix XML markup to be parsed next. May be NULL.
     * @param prefixLength Actual length of `prefix` buffer or `-1` if prefix is NULL terminated.
     * @param body XML markup to be parsed next. Must not be NULL.
     * @param bodyLength Actual length of `body` buffer or `-1` if body is NULL terminated.
     * @param suffix XML markup to be parsed next. May be NULL.
     * @param suffixength Actual length of `suffix` buffer or `-1` if suffix is NULL terminated.
     * @param rules Instructions for integrating new markup within existing markup.
     */
    virtual void injectFragment(const char* uid, const char* prefix, int prefixLength, const char* body, int bodyLength, const char* suffix, int suffixLength, XmlFragmentRules rules) = 0;

    /**
     * Parser extension controlling whether content events for content containing whitespace
     * characters only are ignored (false) or retained (true).
     */
    virtual void keepWhitespace(bool enable) = 0;

    /**
     * Parser extension controlling whether event data returned from `readStartTag`, `readContent`,
     * and `readEndTag` remains valid until the parser is destroyed or its input is reset. If true,
     * pointers returned from these methods are guaranteed to remain valid while parsing. If false,
     * the caller should not rely on the pointers after the next call to `next`.
     */
    virtual void setIndefiniteEventDataRetention(bool enable) = 0;

    /**
     * Provides the parser with an assistant instance for automatic fragment detection.
     */
    virtual void setAssistant(const IFragmentedXmlAssistant* assistant) = 0;
};

/**
 * Instruct the parser to retain a fragment's parent element. When an element identifies a fragment
 * the parser's default behavior is to replace the original element, in its entirety, with the
 * fragment. This instructs the parser to retain the fragment identifying element and inject the
 * fragment markup as descendents of that element.
 */
static const XmlFragmentRules FXPP_RETAIN_FRAGMENT_PARENT   = 0x01;

/**
 * Make all namespaces in effect at the time of injection available to the injected content. Each
 * namespace URI and prefix is defined in the fragment, eliminating the need to repeat definitions
 * in each fragment. In the following example, the use of `foo` in `foo:child` is only valid if
 * `foo:root` explicitly requests namespace propagation. Without propagation, the declaration of
 * `foo` must be repeated in fragment markup.
 *
 *      <foo:root xmlns:foo="urn:foo">
 *        <![CDATA[<foo:child/>]]>
 *      </foo:root>
 * 
 * By default, namespaces are not propagated. Any fragment requiring propagation must request it
 * when constructed.
 */
static const XmlFragmentRules FXPP_PROPAGATE_NAMESPACES     = 0x04;

/**
 * Reserved element name(s) used internally by the parser. Use outside of the parser is permitted
 * subject to the constraints imposed by the parser.
 */
namespace reserved
{
    /**
     * The parser assumes elements with this name are created by the parser for the parser's own
     * use. Neither `START_TAG` nor `END_TAG` events will be reported by the parser to the
     * parser's event consumer.
     */
    constexpr static const char* SYNTHESIZED_ROOT = "synthesized_root";
}

extern IFragmentedXmlPullParser* createParser();

} // namespace fxpp

#endif // FXPP_FRAGMENTED_XML_FULL_PARSER_H_
