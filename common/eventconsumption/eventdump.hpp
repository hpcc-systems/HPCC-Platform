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

#include "eventdump.h"

// Event data may be expressed in multiple structured formats, such as XML, JSON, or YAML. Each of
// these can be represented as in an IPropertyTree. All such representations should share a common
// structure.
//
// The IPropertyTree representation of a binary event file resembles:
//     EventFile
//       ├── Header
//       │   ├── @filename
//       │   ├── @version
//       ├── Event
//       │   ├── @name
//       │   └── ... (other event attributes)
//       ├── ... (more Event elements)
//       └── Footer
//           └── @bytesRead

constexpr static const char* DUMP_STRUCTURE_ROOT = "EventFile";
constexpr static const char* DUMP_STRUCTURE_FILE_NAME = "filename";
constexpr static const char* DUMP_STRUCTURE_HEADER = "Header";
constexpr static const char* DUMP_STRUCTURE_FILE_VERSION = "version";
constexpr static const char* DUMP_STRUCTURE_EVENT = "Event";
constexpr static const char* DUMP_STRUCTURE_EVENT_NAME = "name";
constexpr static const char* DUMP_STRUCTURE_FOOTER = "Footer";
constexpr static const char* DUMP_STRUCTURE_FILE_BYTES_READ = "bytesRead";

// Abstract base class for event visitors that store visited data in a text-based format. Text-
// based, in this context, means only that the values will be stored as strings. The actual output
// format is determined by the subclass.
//
// Subclasses must implement:
//  `virtual bool visitFile(const char* filename, uint32_t version) = 0;`
//  `virtual void departFile(uint32_t bytesRead) = 0;`
//  `virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;`
class CDumpEventVisitor : public CInterfaceOf<IEventVisitor>
{
public: // IEventVisitor
    virtual bool visitEvent(CEvent& event) override
    {
        visitEvent(event.queryType());
        for (CEventAttribute& attr : event.assignedAttributes)
        {
            switch (attr.queryTypeClass())
            {
            case EATCtext:
                visitAttribute(attr.queryId(), attr.queryTextValue());
                break;
            case EATCnumeric:
                visitAttribute(attr.queryId(), attr.queryNumericValue());
                break;
            case EATCboolean:
                visitAttribute(attr.queryId(), attr.queryBooleanValue());
                break;
            default:
                throw makeStringExceptionV(-1, "unsupported attribute type class %u", attr.queryTypeClass());
            }
        }
        departEvent();
        return true;
    }

protected:
    virtual void visitEvent(EventType id) {};

    inline void visitAttribute(EventAttr id, const char* value)
    {
        doVisitAttribute(id, value);
    }

    inline void visitAttribute(EventAttr id, bool value)
    {
        doVisitAttribute(id, value);
    }

    inline void visitAttribute(EventAttr id, __uint64 value)
    {
        doVisitAttribute(id, value);
    }

    virtual void departEvent() {};

protected:
    void doVisitHeader(const char* filename, uint32_t version)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_NAME, filename);
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_VERSION, __uint64(version));
    }

    void doVisitEvent(EventType id)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_EVENT_NAME, queryEventName(id));
    }

    void doVisitAttribute(EventAttr id, __uint64 value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, const char* value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, bool value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, const char* name, __uint64 value)
    {
        if (queryEventAttributeType(id) == EATtimestamp)
        {
            StringBuffer timestamp;
            CDateTime dt;
            dt.setTimeStampNs(value);
            dt.getString(timestamp);
            // assumes CDateTime output is in microseconds
            timestamp.appendf("%03llu", value % 1000);
            recordAttribute(id, name, timestamp, true);
            return;
        }
        recordAttribute(id, name, StringBuffer().append(value), false);
    }

    void doVisitAttribute(EventAttr id, const char* name, const char* value)
    {
        recordAttribute(id, name, value, true);
    }

    void doVisitAttribute(EventAttr id, const char* name, bool value)
    {
        recordAttribute(id, name, (value ? "true" : "false"), false);
    }

    void doVisitFooter(uint32_t bytesRead)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_BYTES_READ, __uint64(bytesRead));
    }

    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;
};
