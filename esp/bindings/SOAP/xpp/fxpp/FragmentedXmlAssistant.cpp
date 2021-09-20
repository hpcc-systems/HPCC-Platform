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

#include <cstring>
#include "fxpp/FragmentedXmlAssistant.hpp"
#include "jmutex.hpp"
#include "jstring.hpp"

using namespace xpp;

namespace fxpp
{

namespace fxa
{

/**
 * IAssistant implementation supporting the registration of elements that may represent either
 * external or embedded fragments.
 * 
 * For embedded fragments, the assistant recognizes which elements may contain embedded fragment
 * data, which of those do contain embedded fragment data, and creates the fragment representing
 * the embedded fragment data.
 *
 * For external fragments, the assistat recognizes which elements may contain external fragment
 * references. For elements that may contain references, it relies upon a separate detector to
 * determine if they do, in fact, contain an external reference. For elements that do contain
 * external references, it relies on a separate loader to resolve the detected key(s) into a
 * fragment with the requested data.
 */
class CAssistant : public CInterfaceOf<IAssistant>
{
private:
    class CInjector : public CInterfaceOf<IExternalContentInjector>
    {
    public:
        virtual bool injectContent(const char* content, int contentLength) override
        {
            if (!m_injector)
                return false;
            m_injector->injectFragment(m_uid, content, contentLength, m_rules);
            return true;
        }

    protected:
        Linked<IFragmentInjector> m_injector;
        StringAttr                m_uid;
        XmlFragmentRules          m_rules;

    public:
        CInjector(IFragmentInjector& injector, const char* uid, XmlFragmentRules rules)
            : m_injector(&injector)
            , m_uid(uid)
            , m_rules(rules)
        {
        }
    };

protected:
    struct Registration
    {
        bool isExternal = false;
        XmlFragmentRules externalRules = 0;
        Linked<const IExternalDetector> externalDetector;
        bool isEmbedded = false;
        XmlFragmentRules embeddedRules = 0;
    };
    using NameRegistrations = std::map<std::string, Registration>;
    using UriRegistrations = std::map<std::string, NameRegistrations>;

public:
    virtual bool resolveExternalFragment(const StartTag& stag, IFragmentInjector& injector) const override
    {
        ReadLockBlock block(m_rwLock);
        const Registration* registration = lookup(stag);

        if (!registration || !registration->isExternal)
            return false;
        Owned<IExternalDetection> detected;
        if (registration->externalDetector)
            detected.setown(registration->externalDetector->detectExternalReference(stag));
        else if (m_defaultDetector)
            detected.setown(m_defaultDetector->detectExternalReference(stag));
        else
            throw XmlPullParserException("missing external fragment detector");
        if (!detected)
            return false;
        if (!m_loader)
            throw XmlPullParserException("missing external fragment loader");
        CInjector loadInjector(injector, detected->queryUID(), registration->externalRules);
        if (!m_loader->loadAndInject(*detected, loadInjector))
            throw XmlPullParserException(VStringBuffer("external fragment load of '%s' failed", detected->queryUID()).str());
        return true;
    }

    virtual bool resolveEmbeddedFragment(const StartTag& stag, const char* content, IFragmentInjector& injector) const override
    {
        if (!mayBeXml(content))
            return false;
        size_t contentLength = strlen(content);
        if (contentLength > INT_MAX)
            throw XmlPullParserException("invalid embedded fragment content length");

        ReadLockBlock block(m_rwLock);
        const Registration* registration = lookup(stag);

        if (!registration || !registration->isEmbedded)
            return false;
        injector.injectFragment(nullptr, content, contentLength, registration->embeddedRules);
        return true;
    }

    virtual void setExternalDetector(const IExternalDetector* detector) override
    {
        WriteLockBlock block(m_rwLock);
        m_defaultDetector.setown(detector);
    }

    virtual void setExternalLoader(const IExternalContentLoader* loader) override
    {
        WriteLockBlock block(m_rwLock);
        m_loader.setown(loader);
    }

    virtual void registerExternalFragment(const char* nsUri,
                                          const char* name,
                                          XmlFragmentRules rules) override
    {
        if (!nsUri || isEmptyString(name))
            throw XmlPullParserException("invalid external fragment registration");

        WriteLockBlock block(m_rwLock);
        Registration& registration = m_uriRegistrations[nsUri][name];
        registration.isExternal = true;
        registration.externalRules = rules;
        registration.externalDetector.clear();
    }

    virtual void registerExternalFragment(const char* nsUri,
                                          const char* name,
                                          XmlFragmentRules rules,
                                          const IExternalDetector& detector) override
    {
        if (!nsUri || isEmptyString(name))
            throw XmlPullParserException("invalid external fragment registration");

        WriteLockBlock block(m_rwLock);
        Registration& registration = m_uriRegistrations[nsUri][name];
        registration.isExternal = true;
        registration.externalRules = rules;
        registration.externalDetector.set(&detector);
    }

    virtual void registerEmbeddedFragment(const char* nsUri,
                                          const char* name,
                                          XmlFragmentRules rules) override
    {
        if (!nsUri || isEmptyString(name))
            throw XmlPullParserException("invalid embedded fragment registration");

        WriteLockBlock block(m_rwLock);
        Registration& registration = m_uriRegistrations[nsUri][name];
        registration.isEmbedded = true;
        registration.embeddedRules = rules;
    }

protected:
    UriRegistrations m_uriRegistrations;
    mutable ReadWriteLock m_rwLock;
    Owned<const IExternalDetector> m_defaultDetector;
    Owned<const IExternalContentLoader> m_loader;

protected:
    virtual const Registration* lookup(const StartTag& stag) const
    {
        const char* uri = stag.getUri();
        UriRegistrations::const_iterator uriIt = m_uriRegistrations.find(uri ? uri : "");
        if (uriIt != m_uriRegistrations.end())
        {
            NameRegistrations::const_iterator nameIt = uriIt->second.find(stag.getLocalName());
            if (nameIt != uriIt->second.end())
                return &nameIt->second;
        }
        return nullptr;
    }

    virtual bool mayBeXml(const char* content) const
    {
        if (!content)
            return false;

        while (isspace(*content))
            content++;
        return ('<' == *content);
    }
};

/**
 * Implementation of `CFragmentedXmlAssistant::IExternalDetector` that relies upon up to three
 * attribute values to identify an external fragment. External fragments may be located by path
 * or resource name. The detector refers to one attribute for path values, a second for resource
 * names, and a third as a secondary key to match a portion of a broader fragment.
 * 
 * Fragments must be located by either the path or resource name attribute. If neither is present,
 * it is assumed that the element does not specify an external fragment. It is an error for both
 * to be present.
 *
 * It is expected that some resources or files will allow a subset of a larger fragment to be
 * matched. The detector can recognize a secondary key value to be used by an external loader.
 *
 * The default resource key attribute is `resource`. The default path key attribute is `path`.
 * The default secondary key attribute is `match`. All defaults may be independenty replaced to
 * satisfy markup syntax requirements.
 */
class CExternalFragmentDetector : public CInterfaceOf<IExternalDetector>
{
protected:
    /**
     * Implementation of `CFragmentedXmlAssistant::IExternalDetection` that represents the
     * identification of a requested external fragment. It holds the identifying information
     * so that it may be passed on to an external loader to be resolved.
     */
    class Detection : public CInterfaceOf<IExternalDetection>
    {
    public:
        virtual int         queryDetectedType() const override
        {
            return m_detectedType;
        }
        virtual const char* queryContentLocation() const override
        {
            return m_location;
        }

        virtual const char* queryContentLocationSubset() const override
        {
            return m_subset;
        }

        virtual const char* queryUID() const override
        {
            return m_uid;
        }

    protected:
        int          m_detectedType = ExternalDetectionType::Unknown;
        StringAttr   m_location;
        StringAttr   m_subset;
        StringBuffer m_uid;

    public:
        Detection(int detectedType, const char* location, const char* subset)
            : m_detectedType(detectedType)
            , m_location(location)
            , m_subset(subset)
        {
            m_uid.appendf("%d.%s.%s", detectedType, (location ? location : ""), (subset ? subset : ""));
        }
    };

public:
    virtual Detection* detectExternalReference(const StartTag& stag) const override
    {
        const char* location = nullptr;
        const char* subset = nullptr;
        int         type = ExternalDetectionType::Unknown;
        for (const Keys::value_type& lvt : m_keys)
        {
            const char* tmpLocation = stag.getValue(lvt.first.c_str());
            if (!tmpLocation)
                continue;

            const char* matchedSubset = nullptr;
            int         matchedType = ExternalDetectionType::Unknown;
            int         defaultType = ExternalDetectionType::Unknown;
            for (const std::map<std::string, int>::value_type& svt : lvt.second)
            {
                if (svt.first.empty())
                {
                    defaultType = svt.second;
                }
                else
                {
                    const char* tmpSubset = stag.getValue(svt.first.c_str());
                    if (!tmpSubset)
                        continue;
                    if (matchedSubset)
                        throw XmlPullParserException("invalid markup - ambigous external fragment reference");
                    matchedSubset = tmpSubset;
                    matchedType = svt.second;
                }
            }

            if ((ExternalDetectionType::Unknown == matchedType) && (ExternalDetectionType::Unknown == defaultType))
                continue;
            if (location)
                throw XmlPullParserException("invalid markup - ambiguous external fragment reference");
            location = tmpLocation;
            if (matchedType != ExternalDetectionType::Unknown)
            {
                subset = matchedSubset;
                type = matchedType;
            }
            else
            {
                subset = nullptr;
                type = defaultType;
            }
        }
        if (isEmptyString(location))
            return nullptr;
        return new Detection(type, location, subset);
    }

protected:
    using Keys = std::map<std::string, std::map<std::string, int> >;
    Keys m_keys;

public:
    CExternalFragmentDetector(const std::initializer_list<DetectionKey>& keys)
    {
        static const std::initializer_list<DetectionKey> defaultKeys({
            { "resource", "", ExternalDetectionType::Resource },
            { "resource", "match", ExternalDetectionType::Resource },
            { "path", "", ExternalDetectionType::Path },
            { "path", "match", ExternalDetectionType::Path },
        });

        initializeKeys(keys.size() ? keys : defaultKeys);
    }

private:
    void initializeKeys(const std::initializer_list<DetectionKey>& keys)
    {
        for (const DetectionKey& k : keys)
        {
            if (isEmptyString(k.location) || (ExternalDetectionType::Unknown == k.type))
                throw XmlPullParserException("invalid fragment detection key");
            std::map<std::string, int>& subsets = m_keys[k.location];
            int& type = subsets[k.subset ? k.subset : ""];
            if (type != ExternalDetectionType::Unknown)
                throw XmlPullParserException("duplicate detector key");
            type = k.type;
        }
    }
};

IAssistant* createAssistant()
{
    return new CAssistant();
}

IExternalDetector* createExternalDetector(const std::initializer_list<DetectionKey>& keys)
{
    return new CExternalFragmentDetector(keys);
}

} // namespace fxa

} // namespace fxpp
