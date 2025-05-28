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
    CDumpStreamEventVisitor(IBufferedSerialOutputStream& _out)
        : out(&_out)
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

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(queryEventName(id));
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
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

IEventAttributeVisitor* createDumpTextEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpTextEventVisitor(out);
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

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(DUMP_STRUCTURE_EVENT);
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
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

IEventAttributeVisitor* createDumpXMLEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpXMLEventVisitor(out);
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

    virtual Continuation visitEvent(EventType id) override
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
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
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

IEventAttributeVisitor* createDumpJSONEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpJSONEventVisitor(out);
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

    virtual Continuation visitEvent(EventType id) override
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
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
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

IEventAttributeVisitor* createDumpYAMLEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpYAMLEventVisitor(out);
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
            markup.append(",");
            encodeCSVColumn(markup, queryEventAttributeName(EventAttr(a)));
        }
        dump(true);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
            state = State::Events;
        encodeCSVColumn(markup, queryEventName(id));
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            markup.append(",");
            if (row[a].get())
                encodeCSVColumn(markup, row[a].get());
        }
        dump(true);
        // Prepare for the next event by clearing only those row values set by the departed event.
        // Header attributes are not cleared so that they can be output for each event.
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            if (row[a].get() && !headerAttrs.count(EventAttr(a)))
                row[a].clear();
        }
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        if (id != EvAttrNone)
        {
            row[id].set(value);
            if (inHeader())
                headerAttrs.insert(id);
        }
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

private:
    StringAttr row[EvAttrMax];
    std::set<EventAttr> headerAttrs;
};

IEventAttributeVisitor* createDumpCSVEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpCSVEventVisitor(out);
}
