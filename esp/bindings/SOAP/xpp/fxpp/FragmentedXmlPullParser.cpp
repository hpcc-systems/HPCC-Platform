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
#include "fxpp/FragmentedXmlPullParser.hpp"
#include <list>
#include <map>
#include <memory>
#include <set>
#include <vector>

using namespace xpp;

namespace fxpp
{

/**
 * Named type for internal parser rules related to fragment injection. Internal rules do not
 * use bits that overlap potential external rules, to allow an internal combination of both.
 */
using XmlInternalFragmentRules = uint16_t;

/**
 * Instruct the parser to require a non-empty unique identifier for a fragment. The identifier is
 * used to detect circular references between fragments which means that the same identifier must
 * be generated for each fragment.
 *
 * Identifiers are required for all fragments with the exception of the initial parser input and
 * embedded fragments. In both of these cases, the fragment content cannot close a loop and content
 * leading to a loop will be detected when an external fragment is applied.
 */
static const XmlInternalFragmentRules FXPP_REQUIRE_FRAGMENT_UID     = 0x0100;

/**
 * Instruct the parser to enforce the injection restriction requiring an in-progress element.
 * Fragments set as parser input and requested from an assistant do not not require this check.
 */
static const XmlInternalFragmentRules FXPP_REQUIRE_CURRENT_ELEMENT  = 0x0200;

/**
 * `IFagmentedXmlPullParser` implementation that uses a stack of `XmlPullParser` instances to
 * support parsing fragments within fragments. To a great extent, it is a wrapper of
 * `XmlPullParser`, and what works with the original parser should work with this class with no
 * required changes. There are exceptions:
 * 
 * - Mixed content, the ability for an element to contain both text and child element content, is
 *   explicitly not supported. Attempting to enable this mode will result in an exception.
 * - Whitespace content events are ignored by default.
 */
class CFragmentedXmlPullParser : public IFragmentedXmlPullParser
{
protected:
    struct DataFrame : public CInterface
    {
        enum State
        {
            New,
            Pulled,
            TypeReported,
            DataReported,
            FrameIgnored,
        };
        int eventType = 0;
        State state = New;
        StartTag stag;
        const char* content = nullptr;
        bool ignorable = false;
        EndTag etag;
    };

    using DataFrames = std::list<Owned<DataFrame> >;

    struct FragmentFrame : extends CInterface
    {
        DataFrames                      pendingData;
        std::shared_ptr<IXmlPullParser> xpp;
        StringBuffer                    uid;
        XmlFragmentRules                rules;

        FragmentFrame(std::shared_ptr<IXmlPullParser>& _xpp, const char* _uid, XmlFragmentRules _rules) : xpp(_xpp), uid(_uid), rules(_rules) {}
        FragmentFrame() : rules(0) {}
    };

    using FragmentStack = std::list<Owned<FragmentFrame> >;
    using ParserArchive = std::list<std::shared_ptr<IXmlPullParser> >;

private:
    /**
     * Implementation of `IFragmentInjector` for internal use by the parser. The parser passes an
     * instance to its assistant when requesting injected fragments. It creates at most one
     * fragment without injecting it; injection is left to the parser.
     */
    class CInjector : public CInterfaceOf<IFragmentInjector>
    {
    public:
        virtual void injectFragment(const char* uid, const char* content, int contentLength, XmlFragmentRules rules) override
        {
            if (m_frame)
                throw XmlPullParserException("at most one injection permitted per resolution request");
            int plen = 0, slen = 0;
            m_frame.setown(m_parser.spawnFragment(uid, nullptr, plen, content, contentLength, nullptr, slen, m_rules | rules));
        }

        CFragmentedXmlPullParser& m_parser;
        Owned<FragmentFrame>      m_frame;
        XmlInternalFragmentRules  m_rules;

        CInjector(CFragmentedXmlPullParser& parser)
            : m_parser(parser)
        {
        }

        CInjector& withInternalRules(XmlInternalFragmentRules rules)
        {
            m_rules = rules & 0xFF00;
            return *this;
        }
    };

public:
    virtual void setInput(const char* content, int contentLength) override
    {
        setInput(nullptr, 0, content, contentLength, nullptr, 0);
    }

    virtual void setInput(const char* prefix, int prefixLength, const char* body, int bodyLength, const char* suffix, int suffixLength) override
    {
        m_activeFragments.clear();
        m_parsers.clear();
        m_activeElements.clear();
        m_deferred.clear();
        (void)createAndUseFragment(nullptr, prefix, prefixLength, body, bodyLength, suffix, suffixLength, 0);
    }

    virtual int next() override
    {
        if (m_deferred)
        {
            pushFragment(m_deferred.getClear());
        }

        while (true)
        {
            FragmentFrame& fragmentRef = fragmentFrameRef();
            DataFrame* df = nextDataFrame();
            switch (df->eventType)
            {
            case START_TAG:
                if (streq(df->stag.getLocalName(), reserved::SYNTHESIZED_ROOT))
                {
                    pushActiveElement(df, DataFrame::FrameIgnored);
                    continue;
                }
                if (m_assistant)
                {
                    XmlInternalFragmentRules internalRules = 0;
                    uint8_t laIndex = 0;
                    bool    isEmbedded = false;
                    DataFrame* la = nullptr;
                    CInjector injector(*this);
                    if (m_assistant->resolveExternalFragment(df->stag, injector.withInternalRules(FXPP_REQUIRE_FRAGMENT_UID)))
                    {
                        // external fragment is ready for use, if use is allowed
                    }
                    else if (((la = peekDataFrame(++laIndex)) != nullptr) &&
                        (CONTENT == la->eventType) &&
                        (m_assistant->resolveEmbeddedFragment(df->stag, la->content, injector.withInternalRules(0))))
                    {
                        // embedded fragment is ready for use, if use is allowed
                        la->state = DataFrame::FrameIgnored;
                        isEmbedded = true;
                    }
                    else
                    {
                        // no fragment 
                        pushActiveElement(df, DataFrame::TypeReported);
                        return df->eventType;
                    }
                    // fragment usage is only allowed when the data frame triggering the fragment
                    // is followed by an end tag
                    la = peekDataFrame(++laIndex);
                    if (CONTENT == la->eventType &&
                        (isEmptyString(la->content) || parserRef().whitespaceContent()))
                    {
                        // empty elements that are not self-closing may report an empty or white
                        // space content event that shouldn't affect the usage check
                        la = peekDataFrame(++laIndex);
                    }
                    if (la->eventType != END_TAG)
                    {
                        if (isEmbedded)
                            throw XmlPullParserException("embedded fragment content must have no siblings");
                        else
                            throw XmlPullParserException("external fragment references must have no content");
                    }
                    // apply the fragment
                    if (testRule(FXPP_RETAIN_FRAGMENT_PARENT, injector.m_frame->rules))
                    {
                        m_deferred.setown(injector.m_frame.getLink());
                        pushActiveElement(df, DataFrame::TypeReported);
                        return df->eventType;
                    }
                    else
                    {
                        do
                        {
                            la = peekDataFrame(laIndex);
                            if (la)
                                la->state = DataFrame::FrameIgnored;
                        }
                        while (laIndex-- != 0);
                        pushFragment(injector.m_frame.getLink());
                    }
                }
                else
                {
                    pushActiveElement(df, DataFrame::TypeReported);
                    return df->eventType;
                }
                break;

            case CONTENT:
                df->state = DataFrame::TypeReported;
                return df->eventType;
            
            case END_TAG:
                if (!m_activeElements.empty())
                {
                    Linked<DataFrame> ae(m_activeElements.back());
                    if (!streq(df->etag.getQName(), ae->stag.getQName()))
                        throw XmlPullParserException(VStringBuffer("element stack mismatch; %s is not closed by %s", ae->stag.getQName(), df->etag.getQName()).str());
                    m_activeElements.pop_back();
                    
                    switch (ae->state)
                    {
                    case DataFrame::DataReported:
                    case DataFrame::TypeReported:
                        df->state = DataFrame::TypeReported;
                        m_reportedElementDepth--;
                        return df->eventType;
                    default:
                        df->state = ae->state;
                        break;
                    }
                }
                else
                    throw XmlPullParserException(VStringBuffer("element stack mismatch; unbalanced end tag %s", df->etag.getQName()).str());
                break;

            case END_DOCUMENT:
                if (fragmentFrameDepth() == 1)
                {
                    df->state = DataFrame::TypeReported;
                    return df->eventType;
                }
                df->state = DataFrame::FrameIgnored;
                popFragmentFrame();
                break;
            }
        }
        throw XmlPullParserException("reached unreachable code in next()");
    }
    
    virtual const char* readContent() override
    {
        DataFrame* df = peekDataFrame();
        if (!df)
            throw XmlPullParserException("no current data frame for content read");
        if (df->state != DataFrame::TypeReported)
            throw XmlPullParserException(VStringBuffer("invalid data frame state (%d) for content read", df->state).str());
        if (df->eventType != CONTENT)
            throw XmlPullParserException(VStringBuffer("invalid data frame event type (%d) for content read", df->eventType).str());
        df->state = DataFrame::DataReported;
        return df->content;
    }
    
    virtual void readEndTag(EndTag& etag) override
    {
        DataFrame* df = peekDataFrame();
        if (!df)
            throw XmlPullParserException("no current data frame for end tag read");
        if (df->state != DataFrame::TypeReported)
            throw XmlPullParserException(VStringBuffer("invalid data frame state (%d) for end tag read", df->state).str());
        if (df->eventType != END_TAG)
            throw XmlPullParserException(VStringBuffer("invalid data frame event type (%d) for end tag read", df->eventType).str());
        df->state = DataFrame::DataReported;
        etag = df->etag;
    }
    
    virtual void readStartTag(StartTag& stag) override
    {
        DataFrame* df = peekDataFrame();
        if (!df)
            throw XmlPullParserException("no current data frame for start tag read");
        if (df->state != DataFrame::TypeReported)
            throw XmlPullParserException(VStringBuffer("invalid data frame state (%d) for start tag read", df->state).str());
        if (df->eventType != START_TAG)
            throw XmlPullParserException(VStringBuffer("invalid data frame event type (%d) for start tag read", df->eventType).str());
        df->state = DataFrame::DataReported;
    	stag = df->stag;
    }
    
    virtual void setMixedContent(bool enable) override
    {
        throw XmlPullParserException("mixed content mode is unsupported");
    }
    
    virtual const SXT_CHAR* getQNameLocal(const SXT_CHAR* qName) const override
    {
        return parserRef().getQNameLocal(qName);
    }
    
    virtual const SXT_CHAR* getQNameUri(const SXT_CHAR* qName) const override
    {
        return parserRef().getQNameUri(qName);
    }
    
    virtual int getNsCount() const override
    {
        return parserRef().getNsCount();
    }
    
    virtual map<string, const SXT_CHAR*>::const_iterator getNsBegin() const override
    {
        return parserRef().getNsBegin();
    }
    
    virtual map<string, const SXT_CHAR*>::const_iterator getNsEnd() const override
    {
        return parserRef().getNsEnd();
    }
    
    virtual void setSupportNamespaces(bool enable) override
    {
        m_supportNamespaces = enable;
        IXmlPullParser* cur = parser();
        if (cur)
            cur->setSupportNamespaces(enable);
    }
    
    virtual bool skipSubTreeEx() override
    {
        bool skippedChildren = false;
        (void)doSkipSubTree(&skippedChildren);
        return skippedChildren;
    }
    
    virtual int skipSubTree() override
    {
        return doSkipSubTree(nullptr);
    }

    virtual const SXT_STRING getPosDesc() const override
    {
        return parserRef().getPosDesc();
    }
    
    virtual int getLineNumber() const override
    {
        return parserRef().getLineNumber();
    }
    
    virtual int getColumnNumber() const override
    {
        return parserRef().getColumnNumber();
    }
    
    virtual const bool whitespaceContent() const override
    {
        DataFrame* df = peekDataFrame();
        if (!df)
            throw XmlPullParserException("no current data frame for whitespace check");
        if (!(DataFrame::TypeReported == df->state || DataFrame::DataReported == df->state))
            throw XmlPullParserException(VStringBuffer("invalid data frame state (%d) for whitespace check", df->state).str());
        if (df->eventType != CONTENT)
            throw XmlPullParserException(VStringBuffer("invalid data frame event type (%d) for whitespace check", df->eventType).str());
        return df->ignorable;
    }

    virtual void keepWhitespace(bool enable) override
    {
        m_keepWhitespace = enable;
    }

    virtual void setIndefiniteEventDataRetention(bool enable) override
    {
        m_indefiniteRetention = enable;
    }

    virtual void setAssistant(const IFragmentedXmlAssistant* assistant) override
    {
        m_assistant.set(assistant);
    }

    virtual void injectFragment(const char* uid, const char* content, int contentLength, XmlFragmentRules rules) override
    {
        injectFragment(uid, nullptr, 0, content, contentLength, nullptr, 0, rules);
    }

    virtual void injectFragment(const char* uid, const char* prefix, int prefixLength, const char* body, int bodyLength, const char* suffix, int suffixLength, XmlFragmentRules rules) override
    {
        createAndUseFragment(uid, prefix, prefixLength, body, bodyLength, suffix, suffixLength, FXPP_REQUIRE_FRAGMENT_UID | FXPP_REQUIRE_CURRENT_ELEMENT | rules);
    }

protected:
    Linked<const IFragmentedXmlAssistant> m_assistant;
    FragmentStack                         m_activeFragments;
    DataFrames                            m_activeElements;
    ParserArchive                         m_parsers;
    unsigned                              m_reportedElementDepth = 0;
    bool                                  m_supportNamespaces = false;
    bool                                  m_keepWhitespace = false;
    bool                                  m_indefiniteRetention = true;
    Owned<FragmentFrame>                  m_deferred;
    
public:
    CFragmentedXmlPullParser()
    {
    }

    CFragmentedXmlPullParser(IFragmentedXmlAssistant& assistant)
        : m_assistant(&assistant)
    {
    }

    ~CFragmentedXmlPullParser()
    {
    }

protected:
    void createAndUseFragment(const char* uid, const char* prefix, int& prefixLength, const char* body, int& bodyLength, const char* suffix, int& suffixLength, XmlInternalFragmentRules rules)
    {
        m_deferred.setown(spawnFragment(uid, prefix, prefixLength, body, bodyLength, suffix, suffixLength, rules));
    }

    FragmentFrame* spawnFragment(const char* uid, const char* prefix, int& prefixLength, const char* body, int& bodyLength, const char* suffix, int& suffixLength, XmlInternalFragmentRules rules)
    {
        checkProposedFragment(uid, prefix, prefixLength, body, bodyLength, suffix, suffixLength, rules);
        
        std::shared_ptr<IXmlPullParser> parser(new XmlPullParser());
        parser->setMixedContent(false);
        parser->setSupportNamespaces(m_supportNamespaces);

        StringBuffer prefixBuf, suffixBuf;
        prepareInputPrefix(prefixBuf, testRule(FXPP_PROPAGATE_NAMESPACES, rules));
        if (prefix)
            prefixBuf.append(prefix);
        if (suffix)
            suffixBuf.append(suffix);
        prepareInputSuffix(suffixBuf);
        parser->setInput(prefixBuf, int(prefixBuf.length()), body, bodyLength, suffixBuf, int(suffixBuf.length()));
        
        return (new FragmentFrame(parser, uid, rules));
    }

    void checkProposedFragment(const char* uid, const char* prefix, int& prefixLength, const char* body, int& bodyLength, const char* suffix, int& suffixLength, XmlInternalFragmentRules rules)
    {
        // Accept lengths of `-1` as requests to compute the actual length.
        #define LENGTH_ADJUSTMENT(c, l) if (-1 == (l) && (c)) l = int(strlen(c))
        LENGTH_ADJUSTMENT(prefix, prefixLength);
        LENGTH_ADJUSTMENT(body, bodyLength);
        LENGTH_ADJUSTMENT(suffix, suffixLength);
        #undef LENGTH_ADJUSTMENT

        StringBuffer errReason;
        recordFailure(testRule(FXPP_REQUIRE_FRAGMENT_UID, rules) && isEmptyString(uid), "invalid UID", errReason);
        recordFailure(isEmptyString(body), "invalid content buffer", errReason);
        recordFailure(bodyLength <= 0, "invalid content length", errReason);
        recordFailure((isEmptyString(prefix) != (0 == prefixLength)), "invalid prefix length", errReason);
        recordFailure((isEmptyString(suffix) != (0 == suffixLength)), "invalid suffix length", errReason);
        recordFailure((isEmptyString(prefix) != isEmptyString(suffix)), "mismatched prefix and suffix", errReason);
        recordFailure(m_deferred != nullptr, "invalid double injection", errReason);
        recordFailure(testRule(FXPP_REQUIRE_CURRENT_ELEMENT, rules) && (0 == m_reportedElementDepth), "invalid injection outside of parent", errReason);
        if (!isEmptyString(uid))
        {
            for (const Owned<FragmentFrame>& ff : m_activeFragments)
            {
                if (ff->uid && streq(ff->uid, uid))
                {
                    VStringBuffer tmp("circular fragment reference with UID '%s'", uid);
                    recordFailure(true, tmp, errReason);
                    break;
                }
            }
        }
        if (!errReason.isEmpty())
        {
            VStringBuffer errMessage("XML fragment injection failed: %s", errReason.str());
            throw XmlPullParserException(errMessage.str());
        }
    }

    inline bool testRule(XmlInternalFragmentRules rule, XmlInternalFragmentRules rules) const
    {
        return ((rules & rule) == rule);
    }

    inline void recordFailure(bool failed, const char* reason, StringBuffer& reasons) const
    {
        if (failed)
        {
            if (!reasons.isEmpty())
                reasons.append(", ");
            reasons.append(reason);
        }
    }

    void prepareInputPrefix(StringBuffer& prefix, bool withNamespaces) const
    {
        if (withNamespaces && fragmentFrameDepth())
        {
            using NsMap = std::map<std::string, const SXT_CHAR*>;
            IXmlPullParser* parser = m_activeFragments.front()->xpp.get();
            StringBuffer attrName;
            appendXMLOpenTag(prefix, reserved::SYNTHESIZED_ROOT, nullptr, false);
            for (NsMap::const_iterator it = parser->getNsBegin(); it != parser->getNsEnd(); ++it)
            {
                if (it->first.compare("xml") == 0)
                    continue;
                if (it->first.empty())
                    attrName.set("xmlns");
                else
                    attrName.setf("xmlns:%s", it->first.c_str());
                appendXMLAttr(prefix, attrName, it->second);
            }
            prefix.append('>');
        }
        else
        {
            appendXMLOpenTag(prefix, reserved::SYNTHESIZED_ROOT, nullptr, true);
        }
    }

    void prepareInputSuffix(StringBuffer& suffix) const
    {
        appendXMLCloseTag(suffix, reserved::SYNTHESIZED_ROOT);
    }

    /**
     * Implement own skip logic to ensure internal state integrity.
     */
    int doSkipSubTree(bool* skippedChildren)
    {
        bool hasChildren = false;
        if (m_deferred)
        {
            if (fragmentFrameDepth() == 0)
            {
                // Deferred with empty stack means we haven't started parsing the input. Install
                // the fragment so it can be properly skipped.
                pushFragment(m_deferred.getLink());
            }
            else
            {
                // Deferred with non-empty stack means we are skipping the deferred fragment's
                // parent element. The fragment is presumed to represent at least one child but
                // should not need to be parsed.
                hasChildren = true;
            }
            m_deferred.clear();
        }

        DataFrame* df = nullptr;
        // Expect an equal number of start and end tags before a start tag has been read, and one
        // extra end tag after reading a start tag.
        int level = (m_activeElements.empty() ? 0 : 1);
        bool done = false;
        while (!done)
        {
            df = nextDataFrame();
            df->state = DataFrame::FrameIgnored;
            switch (df->eventType)
            {
            case START_TAG:
                ++level;
                hasChildren = true;
                break;
            case END_TAG:
                --level;
                if (level <= 0)
                    done = true;
                break;
            case END_DOCUMENT:
                done = true;
                break;
            }
        }
        if (level < 0)
            throw XmlPullParserException("unbalanced end tag while skipping");
        if (level > 0)
            throw XmlPullParserException("unexpected EOF while skipping");
        if (!m_activeElements.empty())
            m_activeElements.pop_back();
        if (skippedChildren)
            *skippedChildren = hasChildren;
        return END_TAG;
    }
    
    size_t fragmentFrameDepth() const
    {
        return m_activeFragments.size();
    }

    FragmentFrame& fragmentFrameRef()
    {
        if (m_activeFragments.empty())
            throw XmlPullParserException("no element markup available");
        return *m_activeFragments.back();
    }

    IXmlPullParser* parser() const
    {
        if (m_activeFragments.empty())
            return nullptr;
        return m_activeFragments.back()->xpp.get();
    }
    
    IXmlPullParser& parserRef() const
    {
        if (m_activeFragments.empty())
            throw XmlPullParserException("no element markup available");
        IXmlPullParser* xpp = m_activeFragments.back()->xpp.get();
        if (!xpp)
            throw XmlPullParserException("invalid parser stack entry");
        return *xpp;
    }

    DataFrames& pendingData()
    {
        if (m_activeFragments.empty())
            throw XmlPullParserException("no element markup available");
        return m_activeFragments.back()->pendingData;
    }

    const DataFrames& pendingData() const
    {
        if (m_activeFragments.empty())
            throw XmlPullParserException("no element markup available");
        return m_activeFragments.back()->pendingData;
    }

    void pushFragment(FragmentFrame* frame)
    {
        StringBuffer errReason;
        recordFailure(!frame, "invalid fragment frame", errReason);
        recordFailure(frame && !frame->xpp, "invalid embedded parser", errReason);
        if (!errReason.isEmpty())
        {
            VStringBuffer errMessage("XML fragment injection failed: %s", errReason.str());
            frame->Release();
            throw XmlPullParserException(errMessage.str());
        }

        if (m_indefiniteRetention)
            m_parsers.emplace_back(frame->xpp);
        m_activeFragments.emplace_back(frame);
    }
    
    void popFragmentFrame()
    {
        if (!m_activeFragments.empty())
            m_activeFragments.pop_back();
    }

    void pullDataFrame()
    {
        IXmlPullParser&  parser = parserRef();
        Owned<DataFrame> frame(new DataFrame());
        bool retry;
        do
        {
            retry = false;
            frame->eventType = parser.next();
            switch (frame->eventType)
            {
            case START_TAG:
                parser.readStartTag(frame->stag);
                break;
            case CONTENT:
                frame->content = parser.readContent();
                frame->ignorable = isEmptyString(frame->content) || parser.whitespaceContent();
                if (!m_keepWhitespace && frame->ignorable)
                {
                    frame->content = nullptr;
                    retry = true; 
                }
                break;
            case END_TAG:
                parser.readEndTag(frame->etag);
                break;
            case END_DOCUMENT:
                break;
            default:
                throw XmlPullParserException(VStringBuffer("unexpected event type %d", frame->eventType).str());
            }
        }
        while (retry);
        
        frame->state = DataFrame::Pulled;
        pendingData().emplace_back(frame.getClear());
    }

    DataFrame* nextDataFrame()
    {
        DataFrames& frames = pendingData();
        while (!frames.empty() && frames.front()->state > DataFrame::Pulled)
            frames.pop_front();
        if (frames.empty())
            pullDataFrame();
        DataFrame* df = frames.front();
        if (df->state != DataFrame::Pulled)
            throw XmlPullParserException(VStringBuffer("invalid data frame state %d", df->state).str());
        return df;
    }

    inline DataFrame* peekDataFrame() const
    {
        const DataFrames& frames = pendingData();
        if (frames.empty())
            throw XmlPullParserException("no data available");
        return frames.front();
    }
    inline DataFrame* peekDataFrame()
    {
        return peekDataFrame(0);
    }
    DataFrame* peekDataFrame(uint8_t frameIdx)
    {
        DataFrames& frames = pendingData();
        while (frames.size() < (frameIdx + 1))
            pullDataFrame();
        for (Owned<DataFrame>& frame : frames)
        {
            if (frameIdx-- == 0)
            {
                return frame;
            }
        }
        throw XmlPullParserException("reached unreachable code in peekDataFrame()");
    }

    void pushActiveElement(DataFrame* df, DataFrame::State state)
    {
        if (df)
        {
            df->state = state;
            if (DataFrame::TypeReported == state)
                m_reportedElementDepth++;
            m_activeElements.push_back(LINK(df));
        }
    }

    void dumpDataFrame(const DataFrame& frame, const char* context) const
    {
        fprintf(stdout, "\n%s: ", context ? context : "no context");
        switch (frame.state)
        {
        case DataFrame::New:
            fprintf(stdout, "New data frame\n");
            return;
        case DataFrame::Pulled:
            fprintf(stdout, "Pulled");
            break;
        case DataFrame::TypeReported:
            fprintf(stdout, "TypeReported");
            break;
        case DataFrame::DataReported:
            fprintf(stdout, "DataReported");
            break;
        case DataFrame::FrameIgnored:
            fprintf(stdout, "FrameIgnored");
            break;
        default:
            fprintf(stdout, "unknown (%d)", frame.state);
        }
        fprintf(stdout, " data frame: ");
        switch (frame.eventType)
        {
        case START_TAG:
            fprintf(stdout, "<%s>", frame.stag.getQName());
            break;
        case CONTENT:
            fprintf(stdout, "<![CDATA[%s]]>", frame.content);
            break;
        case END_TAG:
            fprintf(stdout, "</%s>", frame.etag.getQName());
            break;
        case END_DOCUMENT:
            fprintf(stdout, "EOF");
            break;
        default:
            fprintf(stdout, "unknown (%d)", frame.eventType);
            break;
        }
        fprintf(stdout, "\n");
    }

};

IFragmentedXmlPullParser* createParser()
{
    return new CFragmentedXmlPullParser();
}

} // namespace fxpp
