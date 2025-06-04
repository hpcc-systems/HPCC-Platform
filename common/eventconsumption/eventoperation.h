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
#include "eventfilter.h"

// Extension of `CEventFileOp` that supports streaming output and event filtering by trace ID,
// thread ID, and timestamp range. This satisfies the requirements for the `event_consuming_op_t`
// template parameter of `TEventConsumingCommand`.
//
// Subclasses are expected to add support for additional options.
class event_decl CEventConsumingOp : public CInterface
{
public: // abstract method(s)
    virtual bool doOp() = 0;
public:
    virtual bool ready() const;
    void setInputPath(const char* path);
    void setOutput(IBufferedSerialOutputStream& _out);
    bool acceptEvents(const char* eventNames);
    bool acceptAttribute(EventAttr attr, const char* values);
protected:
    IEventFilter* ensureFilter();
    bool traverseEvents(const char* path, IEventAttributeVisitor& visitor);
protected:
    StringAttr inputPath;
    Linked<IBufferedSerialOutputStream> out;
    Owned<IEventFilter> filter;
};
