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

#pragma once

#include "eventconsumption.h"
#include "eventvisitor.h"
#include "eventoperation.h"

// Get a visitor that streams visited event data in JSON format.
extern event_decl IEventVisitor* createDumpJSONEventVisitor(IBufferedSerialOutputStream& out);

// Get a visitor that streams visited event data in a flat text format.
extern event_decl IEventVisitor* createDumpTextEventVisitor(IBufferedSerialOutputStream& out);

// Get a visitor that streams visited event data in XML format.
extern event_decl IEventVisitor* createDumpXMLEventVisitor(IBufferedSerialOutputStream& out);

// Get a visitor that streams visited event data in YAML format.
extern event_decl IEventVisitor* createDumpYAMLEventVisitor(IBufferedSerialOutputStream& out);

// Get a visitor that streams visited event data in CSV format.
extern event_decl IEventVisitor* createDumpCSVEventVisitor(IBufferedSerialOutputStream& out);

// Encapsulation of a visitor that stores visited event data in a property tree and
// access to the tree.
interface IEventPTreeCreator : extends IInterface
{
    virtual IEventVisitor& queryVisitor() = 0;
    virtual IPTree* queryTree() const = 0;
};

// Get an event property tree creator.
extern event_decl IEventPTreeCreator* createEventPTreeCreator();

// Supported dump output formats.
enum class OutputFormat : byte
{
    text,
    json,
    xml,
    yaml,
    csv,
    tree,
};

// Open and parse a binary event file, generating any one of several supported output formats. As
// an extension of CEventConsumingOp, the public interface adds:
// - `void setFormat(OutputFormat)`: set the output format
// - `bool filterEventType(const char* eventNames)`: add an event type filter to the operation
class event_decl CDumpEventsOp : public CEventConsumingOp
{
public:
    // Cache the requested output format.
    void setFormat(OutputFormat format);

    // Perform the requested action.
    virtual bool doOp() override;

protected:
    OutputFormat format = OutputFormat::text;
};
