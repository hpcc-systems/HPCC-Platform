/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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

#include "evtool.h"
#include "eventoperation.h"
#include "jevent.hpp"
#include "jstring.hpp"

// Abstract implementation of the command interface. `dispatch` is implemented with the help of
// several virtual and abstract methods. `usage` remains for subclasses to implement.
class CEvToolCommand : public CEvtCommandBase
{
public: // new abstract methods
    virtual bool isGoodRequest() = 0;
    virtual int  doRequest() = 0;
public:
    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override;
protected:
    // CEvtCommandBase implementation
    virtual bool acceptArgument(const char* arg) override;
    virtual unsigned consumeArgument(int argc, const char* argv[], int pos) override;
    virtual bool isValidRequest() override { return isGoodRequest(); }
    virtual int executeCommand() override { return doRequest(); }

    // Command line argument processing
    virtual bool accept(const char* arg);
    virtual bool acceptTerseOption(char opt);
    virtual bool acceptVerboseOption(const char* opt);
    virtual bool acceptKVOption(const char* key, const char* value);
    virtual unsigned acceptLongOption(const char* key, const char* nextArg);
    virtual bool acceptParameter(const char* arg);

    // Usage output components
    virtual void usageOptions(IBufferedSerialOutputStream& out) override;
    virtual void usageFilters(IBufferedSerialOutputStream& out);

    // Utility methods
    virtual IPropertyTree* loadConfiguration(const char* path) const;

    // IEvToolCommand description interface (default implementations)
    virtual bool hasVerboseDescription() const override { return true; }
    virtual const char* getVerboseDescription() const override { return nullptr; }
    virtual bool hasBriefDescription() const override { return true; }
    virtual const char* getBriefDescription() const override { return nullptr; }
};

// Extension of `CEvToolCommand` that connects a command to an event file operation. The premise
// is that command connectors derived from this class will be responsible for translating CLI
// arguments into operation parameters. The operations themselves are expected to be independent
// of the command line interface, enabling potential reuse by other tools.
//
// `event_op_t` must implement the following public interface:
// - `bool ready() const`: returns true if the operation has sufficient information to proceed
// - `bool doOp()`: performs the operation and returns true if successful
template <typename event_op_t>
class TEvtCLIConnector : public CEvToolCommand
{
protected:
    using EventOp = event_op_t;
public: // CEvToolCommand
    virtual bool isGoodRequest() override
    {
        return op.ready();
    }
    virtual int doRequest() override
    {
        try
        {
            return op.doOp() ? 0 : 1;
        }
        catch (IException* e)
        {
            StringBuffer msg("command execution exception: ");
            e->errorMessage(msg);
            e->Release();
            msg.append('\n');
            consoleErr().put(msg.length(), msg.str());
            return 1;
        }
    }
protected:
    EventOp op;
};

// Extension of `TEvtCLIConnector` that provides a common base for most, if not all, commands that
// consume event data files. It relies on a single instance of template type `event_consuming_op_t`
// to perform command logic, making this class and its subclasses responsible only for translating
// command line arguments into operation parameters.
//
// `event_consuming_op_t` must also implement the following public interface:
// - `void setInputPath(const char* path)`: sets the operation's input file path, which may be
//   an existing event data file or another file needed by the operation
// - `void setOutput(IBufferedSerialOutputStream& out)`: sets the operation's output stream
// - `bool acceptEvents(const char* events)`: adds an event type filter to the operation
// - `bool acceptAttribute(EventAttr attr, const char* values)`: adds an attribute filter to the
//   operation, where `attr` is any event-specific attribute and `values` is a comma-delimited list
//   of values appropriate for the attribute
// - `bool acceptMetaAttribute(const char* name, const char* values)`: adds a meta-derived
//   attribute filter to the operation, where `name` is one of meta.Path, meta.Plane,
//   meta.LogicalFileName, or meta.ServiceName
template <typename event_consuming_op_t>
class TEventConsumingCommand : public TEvtCLIConnector<event_consuming_op_t>
{
protected:
    using TEvtCLIConnector<event_consuming_op_t>::op;
public:
    TEventConsumingCommand()
    {
        op.setOutput(consoleOut());
    }

protected:
    virtual bool acceptKVOption(const char* key, const char* value) override
    {
        if (streq(key, "events"))
            return op.acceptEvents(value);
        if (strncmp(key, "attribute:", 10) == 0)
        {
            const char* attrName = key + 10;
            if (strncmp(attrName, EVENT_META_PREFIX, sizeof(EVENT_META_PREFIX) - 1) == 0)
                return op.acceptMetaAttribute(attrName, value);
            EventAttr attr = queryEventAttribute(attrName);
            switch (attr)
            {
            case EvAttrNone: // not an attribute
            case EvAttrMax: // not an attribute
                return false;
            default:
                if (attr > EvAttrMax) // should never happen
                    return false;
                break;
            }
            return op.acceptAttribute(attr, value);
        }
        if (streq(key, "model"))
        {
            Owned<IPropertyTree> config = this->loadConfiguration(value); // avoid name collision with jptree
            if (!config)
                return false;
            return op.acceptModel(*config);
        }
        return CEvToolCommand::acceptKVOption(key, value);
    }

    virtual bool acceptParameter(const char* arg) override
    {
        op.setInputPath(arg);
        return true;
    }

    virtual void usageParameters(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Parameters:
    <filename>                Full path to an event data file. One is required.
                              Multiple are accepted, with the effect of merge-
                              sorting the events from each by timestamp.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr =
R"!!!(    --model=<filename>        Apply a model to the data using the specified
                              YAML/XML/JSON configuration file.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        TEvtCLIConnector<event_consuming_op_t>::usageOptions(out);
        out.put(usageStrLength, usageStr);
    }

    virtual void usageFilters(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Filters:
    --events=<term>[,<term>...]
        Filter by event name or context name. Terms are evaluated from left
        to right. Terms may be prefixed with a comparison selector.

        Event selectors (default: [eq]):
            [eq]      include matching event name
            [neq]     include all event names except match
            [except]  exclude the event from the set of prior inclusions;
                      exceptions are only effective when specified after
                      another inclusive term
        Context selectors (default: [in]):
            [in]      include all events matching the context
            [out]     include all events not matching the context

        Examples:
            --events=[eq]QueryStart
            --events=[in]Dali,[except]DaliCommit

        Discover valid names with:
            evtool describe -e   (events)
            evtool describe -c   (contexts)
    --attribute:<name>=<term>[,<term>...]
        Filter by attribute value. Events without the specified attribute are
        unaffected by the filter. Events with the specified attribute must
        satisfy at least one term to avoid suppression.

        <name> is any filterable attribute name from:
            evtool describe -a

        The meaning of <term> depends on attribute value type.

        Multiple comma-delimited terms are combined as a set of allowed terms;
        negated terms exclude matches.

        String-valued attributes:
            A term is a single string optionally prefixed by a comparison
            selector. The string may include wildcard characters for pattern
            matching.

            Selectors (default: [wild]):
                [eq]    case-insensitive exact match
                [neq]   exclude case-insensitive exact match; include all others
                [lt]    case-insensitive lexical less-than comparison
                [lte]   case-insensitive lexical less-than-or-equal comparison
                [gt]    case-insensitive lexical greater-than comparison
                [gte]   case-insensitive lexical greater-than-or-equal comparison
                [wild]  case-sensitive wildcard pattern match

            Examples:
                --attribute:Path=*.csv
                --attribute:ServiceName=[eq]warehouse,[neq]archive

        Boolean-valued attributes:
            A term is true or false with no selector.

        Numeric- and timestamp-valued attributes:
            Timestamps accept yyyy-mm-ddThh:mm:ss[.nnnnnnnnn] text (no timezone
            suffix) and are interpreted as UTC.
            Numeric values are integer-only; unit suffixes are not supported.

            A term is a single value or a hyphen-delimited range, optionally
            prefixed by a comparison selector. Terms are interpreted as ranges
            [N,M], where N and M are unsigned integers, with these shortcuts:
                N      range N-N
                -M     range 0-M
                N-     range N-max
                N-M    range N-M

            Arithmetic Selectors (default: [in]):
                [eq]      matches range [N,N] only
                [neq]     excludes range [N,N] only
                [lt]      matches values less than N
                [lte]     matches values less than or equal to N
                [gt]      matches values greater than M
                [gte]     matches values greater than or equal to M
                [in]      matches values within [N,M]
                [out]     excludes values within [N,M]

            Examples:
                --attribute:ElapsedTime=[in]10-50
                --attribute:ElapsedTime=[out]10-50
                --attribute:EventTimestamp=[lt]2024-01-01T08:01:00

)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};
