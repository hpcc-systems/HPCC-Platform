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
    virtual bool isValidRequest() override { return isGoodRequest(); }
    virtual int executeCommand() override { return doRequest(); }

    // Command line argument processing
    virtual bool accept(const char* arg);
    virtual bool acceptTerseOption(char opt);
    virtual bool acceptVerboseOption(const char* opt);
    virtual bool acceptKVOption(const char* key, const char* value);
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
// - `bool acceptAttributes(EventAttr attr, const char* values)`: adds an attribute filter to the
//   operation, where `attr` is any event-specific attribute and `values` is a comma-delimited list
//   of values appropriate for the attrribute
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
    --events=<events>         Skip events not in the specified comm-delimited
                              event list.
                              Event type names, accepted values are derived
                              from the EventType enumeration:
                                - IndexCacheHit
                                - IndexCacheMiss
                                - IndexLoad
                                - IndexPayload
                                - IndexEviction
                                - DaliChangeMode
                                - DaliCommit
                                - DaliConnect
                                - DaliEnsureLocal
                                - DaliGet
                                - DaliGetChildren
                                - DaliGetChildrenFor
                                - DaliGetElements
                                - DaliSubscribe
                                - FileInformation
                                - RecordingActive
                                - IndexPayload
                              Event context names are also accepted. An event
                              context is a built-in grouping of related events
                              including:
                                - Dali:
                                  - DaliChangeMode
                                  - DaliCommit
                                  - DaliConnect
                                  - DaliEnsureLocal
                                  - DaliGet
                                  - DaliGetChildren
                                  - DaliGetChildrenFor
                                  - DaliGetElements
                                  - DaliSubscribe
                                - Index
                                  - IndexCacheHit
                                  - IndexCacheMiss
                                  - IndexLoad
                                  - IndexPayload
                                  - IndexEviction
                                  - FileInformation
                                  - IndexPayload
                                - Other
                                  - RecordingActive
    --attribute:<attr>=<val>  Skip events the include the specified attribute
                              but which do not have a specified value. Events
                              without the specified attribute are not skipped.
                              <val> may be a comma-delimited list of value
                              tokens. Except as noted below, accepted tokens
                              depend on the attribute value type:
                                - Boolean: true and false as recognized by the
                                    strToBool utility function
                                - Numeric: a single numeric value or a range of
                                    numeric values delimited by a hyphen
                                    - "#-#": a range of numeric values, bounded
                                        on both ends
                                    - "#-": a range of numeric values, bounded
                                        only on the lower end
                                    - "-#": a range of numeric values, bounded
                                        only on the upper end
                                - String: a case-sensitive match or a wildcard
                                  pattern
                              Choices for <attr> are derived from the EventAttr
                              enumeration:
                                - FileId: numeric, but a string token joins the
                                    current event with a preceding occurrence
                                    of MetaFileInformation, by file ID, and
                                    applies the filter to the path.
                                - FileOffset: numeric
                                - NodeKind: numeric (0, 1, or 2) or text
                                    equivalent (branch, leaf, or blob)
                                - ReadTime: numeric
                                - ExpandTime: numeric
                                - InMemorySize: numeric
                                - InCache: Boolean
                                - Path: string
                                - ConnectId: numeric
                                - Enabled: Boolean
                                - FileSize: numeric
                                - EventTimestamp: numeric or fully formed time-
                                    stamp strings as recognized by the
                                    CDateTime::setString method.
                                - EventTraceId: string matching either a recorded
                                    trace ID or an associated query service name
                                - EventThreadId: numeric
                                - EventStackTrace: string
                                - DataSize: numeric
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};
