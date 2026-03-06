/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "eventdump.hpp"
#include <set>

// Abstract extension of CDumpEventVisitor that writes visited data to an output stream. Subclasses
// are responsible for formatting the data in the desired format.
class CDumpStreamEventVisitor : public CDumpEventVisitor
{
protected: // new abstract method(s)
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;

public:
    CDumpStreamEventVisitor(IBufferedSerialOutputStream& _out, CMetaInfoState& _metaState, bool _fullPathOutput)
        : CDumpEventVisitor(_metaState, _fullPathOutput), out(&_out)
    {
    }

protected:
    void dump(bool eoln)
    {
        out->put(markup.length(), markup.str());
        markup.clear();
        if (eoln)
            out->put(1, "\n");
    }

    bool inHeader() const { return State::Header == state; }
    bool inEvents() const { return State::Events == state; }

protected:
    enum class State : byte
    {
        Idle,
        Header,
        Events,
        Footer,
    };
    State state{State::Idle};
    Linked<IBufferedSerialOutputStream> out;
    StringBuffer markup;
};

// Concrete extension of CDumpStreamEventVisitor that writes visited data as unstructured text.
// One line of text is produced for each event and attribute visited, plus additional lines for
// the file name, version, and number of bytes read from the file.
class CDumpTextEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        doVisitHeader(filename, version);
        return true;
    }

    virtual void visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(queryEventName(id));
        doVisitEvent(id);
    }

    virtual void departEvent() override
    {
        closeElement();
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        doVisitFooter(bytesRead);
        closeElement();
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        if (EvAttrNone == id)
            return;
        markup.append("attribute: ").append(name).append(" = ");
        if (quoted)
            markup.append('\'');
        markup.append(value);
        if (quoted)
            markup.append('\'');
        markup.append('\n');
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

protected:
    void openElement(const char* name)
    {
        markup.append("event: ").append(name).append('\n');
    }

    void closeElement()
    {
        dump(false);
    }
};

IEventVisitor* createDumpTextEventVisitor(IBufferedSerialOutputStream& out, CMetaInfoState& metaState, bool fullPathOutput)
{
    return new CDumpTextEventVisitor(out, metaState, fullPathOutput);
}

// Extension of CDumpStreamEventVisitor that outputs XML-formatted text. Header and Footer elements
// synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpXMLEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement(DUMP_STRUCTURE_ROOT, true);
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual void visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(DUMP_STRUCTURE_EVENT);
        doVisitEvent(id);
    }

    virtual void departEvent() override
    {
        closeElement();
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
        closeElement(DUMP_STRUCTURE_ROOT);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        appendXMLAttr(markup, name, value);
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

    void openElement(const char* name, bool complete = false)
    {
        markup.pad(2 * indentLevel);
        indentLevel++;
        appendXMLOpenTag(markup, name, nullptr, complete);
        if (complete)
            markup.append('\n');
    }

    void closeElement()
    {
        markup.append("/>");
        indentLevel--;
        dump(true);
    }

    void closeElement(const char* name)
    {
        appendXMLCloseTag(markup, name);
        indentLevel--;
        dump(true);
    }

private:
    unsigned indentLevel{0};
};

IEventVisitor* createDumpXMLEventVisitor(IBufferedSerialOutputStream& out, CMetaInfoState& metaState, bool fullPathOutput)
{
    return new CDumpXMLEventVisitor(out, metaState, fullPathOutput);
}

// Extension of CDumpStreamEventVisitor that outputs JSON-formatted text. Header and Footer objects
// are synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpJSONEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement();
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual void visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        if (firstEvent)
        {
            openArray(DUMP_STRUCTURE_EVENT);
            openElement();
            firstEvent = false;
        }
        else
            openElement();
        doVisitEvent(id);
    }

    virtual void departEvent() override
    {
        closeElement();
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        else if (inEvents())
            closeArray();
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
        closeElement(true);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        conditionalDelimiter();
        indent();
        appendJSONStringValue(markup, name, value, true, quoted);
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

protected:
    inline void openElement()
    {
        openElement(nullptr);
    }

    void openElement(const char* name)
    {
        openContainer(name, "{ ");
    }

    void openArray(const char* name)
    {
        openContainer(name, "[ ");
    }

    void closeArray()
    {
        closeContainer("]");
    }

    void closeElement(bool done = false)
    {
        closeContainer("}");
        dump(done);
    }

    void openContainer(const char* name, const char* token)
    {
        conditionalDelimiter();
        indent();
        if (!isEmptyString(name))
            appendJSONName(markup, name);
        markup.append(token);
        firstFlags.push_back(true);
    }

    void closeContainer(const char* token)
    {
        firstFlags.pop_back();
        indent();
        markup.append(token);
    }

    void conditionalDelimiter()
    {
        if (!firstFlags.empty())
        {
            if (!firstFlags.back())
                markup.append(",");
            else
                firstFlags.back() = false;
        }
    }

    void indent()
    {
        markup.append('\n');
        if (!firstFlags.empty())
            markup.pad(2 * (firstFlags.size()));
    }

private:
    std::vector<bool> firstFlags;
    bool firstEvent{true};
};

IEventVisitor* createDumpJSONEventVisitor(IBufferedSerialOutputStream& out, CMetaInfoState& metaState, bool fullPathOutput)
{
    return new CDumpJSONEventVisitor(out, metaState, fullPathOutput);
}

// Extension of CDumpStreamEventVisitor that outputs YAML-formatted text. Header and Footer objects
// are synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpYAMLEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual void visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            openElement(DUMP_STRUCTURE_EVENT);
            state = State::Events;
        }
        else
            openElement(nullptr);
        doVisitEvent(id);
    }

    virtual void departEvent() override
    {
        closeElement();
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        indent();
        markup.append(name).append(": ").append(value).append('\n');
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

    void openElement(const char* name)
    {
        if (!isEmptyString(name))
        {
            indent();
            markup.append(name).append(":\n");
        }
        indentLevel++;
        firstProp = true;
    }

    void closeElement()
    {
        indentLevel--;
        dump(false);
    }

    void indent()
    {
        if (indentLevel)
        {
            uint8_t limit = indentLevel;
            if (inEvents() && firstProp)
                limit -= 1;
            markup.pad(2 * limit);
            if (inEvents() && firstProp)
            {
                markup.append("- ");
                firstProp = false;
            }
        }
    }

protected:
    bool firstProp{true};
    uint8_t indentLevel{0};
};

IEventVisitor* createDumpYAMLEventVisitor(IBufferedSerialOutputStream& out, CMetaInfoState& metaState, bool fullPathOutput)
{
    return new CDumpYAMLEventVisitor(out, metaState, fullPathOutput);
}

class CDumpCSVEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {

        state = State::Header;
        encodeCSVColumn(markup, "EventName");
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            if (skipAttribute(a))
                continue;
            markup.append(",");
            encodeCSVColumn(markup, queryEventAttributeName(EventAttr(a)));
        }
        dump(true);
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        if (inHeader())
            state = State::Events;
        encodeCSVColumn(markup, queryEventName(event.queryType()));

        for (const CEventAttribute& attr : event.allAttributes)
        {
            if (skipAttribute(attr.queryId()))
                continue;
            markup.append(",");
            if (attr.isAssigned())
                doVisitAttribute(event, attr);
        }
        dump(true);
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
    }

protected:
    bool skipAttribute(unsigned id) const
    {
        switch (id)
        {
        case EvAttrProcessDescriptor:
            // Only appears in RecordingSource, which is never visited.
            return true;
        case EvAttrEventStackTrace:
            // Never recorded, even when option is set.
            // MORE: revisit if option is implemented
            return true;
        case EvAttrEventThreadId:
            // Skip if not enabled in any input file
            return properties.options.includeThreadIds == EventFileOption::Disabled;
        case EvAttrEventTraceId:
            // Skip if not enabled in any input file
            return properties.options.includeTraceIds == EventFileOption::Disabled;
        default:
            return false;
        }
    }

    virtual void recordAttribute(EventAttr id, const char*, const char* value, bool) override
    {
        if (id != EvAttrNone)
            encodeCSVColumn(markup, value);
    }

public:
    CDumpCSVEventVisitor(IBufferedSerialOutputStream& out, const EventFileProperties& _properties, CMetaInfoState& _metaState, bool _fullPathOutput)
        : CDumpStreamEventVisitor(out, _metaState, _fullPathOutput), properties(_properties)
    {
    }

private:
    EventFileProperties properties;  // Copied to avoid lifetime issues
};

IEventVisitor* createDumpCSVEventVisitor(IBufferedSerialOutputStream& out, const EventFileProperties& properties, CMetaInfoState& metaState, bool fullPathOutput)
{
    return new CDumpCSVEventVisitor(out, properties, metaState, fullPathOutput);
}
