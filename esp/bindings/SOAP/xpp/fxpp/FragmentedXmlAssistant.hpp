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

#ifndef FXPP_FRAGMENTED_XML_ASSISTANT_H
#define FXPP_FRAGMENTED_XML_ASSISTANT_H

#include "fxpp/FragmentedXmlPullParser.hpp"

namespace fxpp
{

namespace fxa
{
/**
 * The `fxpp::fxa` namespace represents one possible implementation of `IFragmentedXmlAssistant`.
 * All declared interfaces may be reused by alternate implementations if such reuse makes sense,
 * but this is not required.
 *
 * To support the detection of embedded fragment content, one or more calls to
 * `IAssistant::registerEmbeddedFragment` must be made prior to parsing markup. Each call
 * designates an additional element URI and name combination that is allowed to contain embedded
 * content.
 *
 * To support the detection of external fragment references, one or more calls to
 * `IAssistant::registerExternalFragment` must be combined with calls to
 * `IAssistant::setExternalDetector` and `IAssistant::setExternalLoader`. The assistant knows when
 * to look for an external reference but delegates locating references to the registered
 * `IExternalDetector` instance and loading referenced content to the registered
 * `IExternalContentLoader` instance. `IExternalDetection` is used to report the detector's
 * findings to the loader. `IExternalContentInjector` is used by the assistant to accept content
 * from the loader.
 */

/**
 * Abstraction representing the "identity" of an external fragment. The identity of a fragment is
 * the information located by a detector that can be used by a loader to get the fragment data. It
 * is assumed that a fragment may be uniquely identified by at least one key and at most two keys.
 * The first, or location, may be a file path or resource bundle identifier. The second, if used,
 * may be an XPath referring to a nodeset within a broader XML document or some other identifier
 * used to refine the data identified by the primary key.
 */
interface IExternalDetection : extends IInterface
{
    virtual int         queryDetectedType() const = 0;
    virtual const char* queryContentLocation() const = 0;
    virtual const char* queryContentLocationSubset() const = 0;
    virtual const char* queryUID() const = 0;

    inline bool matchesType(int type) const { return queryDetectedType() == type; }
};

/**
 * A collection of some well known external reference detection types. This collection may not be
 * comprehensive. It gives names to values with meaning to the implementation and may be extended
 * to include other values, but such extension is not required.
 *
 * - Unknown:  A value has not been assigned. Registered detection keys must not use this value.
 * - Resource: The location value identifies a resource bundle's resource. The subset value is
 *             not currently significant.
 * - Path:     The location value identifies the file system path of an XML file. The subset value
 *             is not currently significant.
 */
enum ExternalDetectionType
{
    Unknown,
    Resource,
    Path,
};

/**
 * Abstraction describing a delegate responsible for locating external fragment reference values
 * within parser start tag structures.
 */
interface IExternalDetector : extends IInterface
{
    virtual IExternalDetection* detectExternalReference(const StartTag& stag) const = 0;
};

/**
 * Abstraction describing a helper responsible for accepting fragment content from an
 * `IExternalContentLoader` instance. An assistant will provide an instance of this interface to
 * a loader instance as the only means of propagating loaded content back to the parser.
 */
interface IExternalContentInjector : extends IInterface
{
    virtual bool injectContent(const char* content, int contentLength) = 0;
};

/**
 * Abstraction describing a delegate responsible for loading detected content for an assistant.
 * It is separate from the assistant to support alternate sources; a unit test might access in-
 * memory data while other use cases might load files or perform resource bundle lookups.
 */
interface IExternalContentLoader : extends IInterface
{
    virtual bool loadAndInject(const IExternalDetection& detected, IExternalContentInjector& injector) const = 0;
};

/**
 * Extension of the `IFragmentedXmlAssistant` interface adding awareness of the delegate
 * interfaces and supported markup knowledge.
 *
 * `registerEmbeddedFragment` is the only required method to support embedded fragments.
 *
 * `setExternalLoader` and `registerExternalFragment` are required to support external fragment
 * references. `setExternalDetector` is also required if any registered fragments do not register
 * an element-specific detector. 
 */
interface IAssistant : extends IFragmentedXmlAssistant
{
    virtual void setExternalDetector(const IExternalDetector* detector) = 0;
    virtual void setExternalLoader(const IExternalContentLoader* loader) = 0;
    virtual void registerExternalFragment(const char* nsUri, const char* name, XmlFragmentRules rules) = 0;
    virtual void registerExternalFragment(const char* nsUri, const char* name, XmlFragmentRules rules, const IExternalDetector& detector) = 0;
    virtual void registerEmbeddedFragment(const char* nsUri, const char* name, XmlFragmentRules rules) = 0;
};

/**
 * Definition of a single rule used by an external detector to locate external fragment references.
 * A collection of these may be provided to `createExternalDetector` to initialize the default
 * detector with alternate rules.
 */
struct DetectionKey
{
    const char* location = nullptr;
    const char* subset = nullptr;
    int         type = ExternalDetectionType::Unknown;

    DetectionKey(const char* _location, const char* _subset, int _type) : location(_location), subset(_subset), type(_type) {}
};

/**
 * Create an instance of the default `IAssistant` implementation.
 */
extern IAssistant* createAssistant();

/**
 * Create an instance of the default `IExternalDetector` implementation. The detection keys soecify
 * all markup-specific detection rules used by the instance. Default rules are not merged with these
 * keys.
 */
extern IExternalDetector* createExternalDetector(const std::initializer_list<DetectionKey>& keys);

/**
 * Create an instance of the default `IExternalDetector` implementation. The absence of detection
 * keys implies a default configuration will be used. The default configuration is:
 *
 * | Location | Subset | Type |
 * | :-: | :-: | :- |
 * | resource | | ExternalDetectionType::Resource |
 * | resource | match | ExternalDetectionType::Resource |
 * | path | | ExternalDetectionType::Path |
 * | path | match | ExternalDetectionType::Path |
 */
inline IExternalDetector* createExternalDetector() { return createExternalDetector({}); }

} // namespace fxa

} // namespace fxpp

#endif // FXPP_FRAGMENTED_XML_ASSISTANT_H