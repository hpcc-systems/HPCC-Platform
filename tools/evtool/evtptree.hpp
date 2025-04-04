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

#include "jiface.hpp"
#include "jevent.hpp"
#include "jptree.hpp"

// The IPropertyTree representation of a binary event file resembles:
//     EventFile
//       │   ├── @filename
//       ├── Header
//       │   ├── @version
//       │   └── ... (other header attributes)
//       ├── Event
//       │   ├── @name
//       │   ├── @id
//       │   └── ... (other event attributes)
//       ├── ... (more Event elements)
//       └── Footer
//           ├── @bytesRead

constexpr static const char* EVT_PTREE_ROOT = "EventFile";
constexpr static const char* EVT_PTREE_FILE_NAME = "filename";
constexpr static const char* EVT_PTREE_HEADER = "Header";
constexpr static const char* EVT_PTREE_FILE_VERSION = "version";
constexpr static const char* EVT_PTREE_EVENT = "Event";
constexpr static const char* EVT_PTREE_EVENT_NAME = "name";
constexpr static const char* EVT_PTREE_EVENT_ID = "id";
constexpr static const char* EVT_PTREE_FOOTER = "Footer";
constexpr static const char* EVT_PTREE_FILE_BYTES_READ = "bytesRead";

class CPTreeEventVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override;
    virtual Continuation visitEvent(EventType id) override;
    virtual Continuation visitAttribute(EventAttr id) override;
    virtual Continuation visitAttribute(EventAttr id, const char* value) override;
    virtual Continuation visitAttribute(EventAttr id, bool value) override;
    virtual Continuation visitAttribute(EventAttr id, uint8_t value) override;
    virtual Continuation visitAttribute(EventAttr id, uint16_t value) override;
    virtual Continuation visitAttribute(EventAttr id, uint32_t value) override;
    virtual Continuation visitAttribute(EventAttr id, uint64_t value) override;
    virtual void leaveFile(uint32_t bytesRead) override;
protected:
    std::ostream& out;
    Owned<IPTree> tree;
    IPTree* active = nullptr;
public:
    CPTreeEventVisitor(std::ostream& _out);
};
