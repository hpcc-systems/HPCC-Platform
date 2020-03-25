/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef _XJXPP_HPP__
#define _XJXPP_HPP__
#include "jlib.hpp"
#include "jptree.hpp"
#include "xjx.hpp"

namespace xpp
{
class CXJXNotifyEvent : implements IPTreeNotifyEvent, public CInterface
{
private:
    StringBuffer m_name;
    StringBuffer m_value;
    int m_eventType;

public:
    IMPLEMENT_IINTERFACE;

    CXJXNotifyEvent() : m_eventType(0) {}
    virtual ~CXJXNotifyEvent() {}

    //IPTreeNotifyEvent
    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override;
    virtual void newAttribute(const char *name, const char *value) override;
    virtual void beginNodeContent(const char *tag) override;
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset) override;

    int getEventType();
    void resetEventType();
    const char* queryValue();
    const char* queryName();
};

class CXJXPullParser : implements XJXPullParser, implements IInterface, public CInterface
{
private:
    bool m_isEndTag;
    bool m_calledNext;
    bool m_reachedEnd;

protected:
    PTreeReaderOptions m_readerOptions;
    Owned<CXJXNotifyEvent> m_event;
    Owned<IPullPTreeReader> m_reader;
    void beforeSetInput();

public:
    IMPLEMENT_IINTERFACE;

    CXJXPullParser(bool supportNameSpaces);
    virtual ~CXJXPullParser() {}

    //Implement common methods
    virtual int next() override;
    virtual const char* readContent() override;
    virtual void readEndTag(EndTag& etag) override;
    virtual void readStartTag(StartTag& stag) override;
};

class CJsonPullParser : public CXJXPullParser
{
public:
    CJsonPullParser(bool supportNameSpaces) : CXJXPullParser(supportNameSpaces) { }
    virtual ~CJsonPullParser() { }

    virtual void setInput(const char* buf, int bufSize) override;
};

class CXmlPullParser : public CXJXPullParser
{
public:
    CXmlPullParser(bool supportNameSpaces) : CXJXPullParser(supportNameSpaces) { }
    virtual ~CXmlPullParser() { }

    virtual void setInput(const char* buf, int bufSize) override;
};

}
#endif
