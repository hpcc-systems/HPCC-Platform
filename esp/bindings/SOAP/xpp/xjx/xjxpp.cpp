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

#include "xjxpp.hpp"
#include <xpp/EndTag.h>
#include <xpp/StartTag.h>
#include <xpp/XmlPullParser.h>

namespace xpp
{
void CXJXNotifyEvent::beginNode(const char *tag, bool arrayitem, offset_t startOffset)
{
    m_eventType = 1;
    m_name.set(tag);
}

void CXJXNotifyEvent::newAttribute(const char *name, const char *value)
{
    m_eventType = 2;
    m_name.set(name);
    m_value.set(value);
}

void CXJXNotifyEvent::beginNodeContent(const char *tag)
{
    m_eventType = 3;
    m_name.set(tag);
}

void CXJXNotifyEvent::endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
{
    m_name.set(tag);
    m_value.clear();
    if (length == 0)
        m_eventType = 4;
    else
    {
        m_eventType = 5;
        m_value.append(length, (const char*)value);
    }
}

int CXJXNotifyEvent::getEventType()
{
    return m_eventType;
}

void CXJXNotifyEvent::resetEventType()
{
    m_eventType = 0;
}

const char* CXJXNotifyEvent::queryValue()
{
    return m_value.str();
}

const char* CXJXNotifyEvent::queryName()
{
    return m_name.str();
}

CXJXPullParser::CXJXPullParser(bool supportNameSpaces) : m_isEndTag(false), m_calledNext(false), m_reachedEnd(false)
{
    if(supportNameSpaces)
        m_readerOptions = ptr_ignoreWhiteSpace;
    else
        m_readerOptions = static_cast<PTreeReaderOptions>(ptr_ignoreWhiteSpace | ptr_ignoreNameSpaces);
}

int CXJXPullParser::next()
{
    if (m_isEndTag)
    {
        m_isEndTag = false;
        return XmlPullParser::END_TAG;
    }

    while ((m_calledNext && !m_reachedEnd) || m_reader->next())
    {
        if (m_calledNext)
            m_calledNext = false;
        int eventType = m_event->getEventType();
        m_event->resetEventType();
        if (eventType == 1)
            return XmlPullParser::START_TAG;
        else if (eventType == 4)
            return XmlPullParser::END_TAG;
        else if (eventType == 5)
        {
            m_isEndTag = true;
            return XmlPullParser::CONTENT;
        }
    }

    return XmlPullParser::END_DOCUMENT;
}

const char* CXJXPullParser::readContent()
{
    return m_event->queryValue();
}

void CXJXPullParser::readEndTag(EndTag& tag)
{
    const char* name = m_event->queryName();
    if (!name || !*name)
        return;
    tag.nameBuf = name;
    tag.qName = tag.nameBuf.c_str();
    const char* colon = strchr(tag.qName, ':');
    if (colon)
        tag.localName = colon+1;
    else
        tag.localName = tag.qName;
}

void CXJXPullParser::readStartTag(StartTag& tag)
{
    const char* name = m_event->queryName();
    if (!name || !*name)
        return;
    tag.nameBuf = name;
    tag.qName = tag.nameBuf.c_str();
    const char* colon = strchr(tag.qName, ':');
    if (colon)
        tag.localName = colon+1;
    else
        tag.localName = tag.qName;

    while(true)
    {
        bool hasMore = m_reader->next();
        m_calledNext = true;
        if (!hasMore)
        {
            m_reachedEnd = true;
            break;
        }
        int eventType = m_event->getEventType();
        m_event->resetEventType();
        if (eventType != 2)
            break;
        tag.ensureCapacity(tag.attEnd+1);
        const char* name = m_event->queryName();
        if (name && *name=='@')
            name++;
        tag.attArr[tag.attEnd].qName = name;
        tag.attArr[tag.attEnd].value = m_event->queryValue();
        tag.attEnd++;
    }
}

void CXJXPullParser::beforeSetInput()
{
    m_event.setown(new CXJXNotifyEvent());
    m_isEndTag = false;
    m_calledNext = false;
    m_reachedEnd = false;
}

void CJsonPullParser::setInput(const char* buf, int bufSize)
{
    beforeSetInput();
    m_reader.setown(createPullJSONBufferReader(buf, bufSize, *m_event.get(), m_readerOptions));
}

void CXmlPullParser::setInput(const char* buf, int bufSize)
{
    beforeSetInput();
    m_reader.setown(createPullXMLBufferReader(buf, bufSize, *m_event.get(), m_readerOptions));
}

}
