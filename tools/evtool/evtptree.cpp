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

#include "evtptree.hpp"
#include "jstring.hpp"

#define ATTR(name) VStringBuffer("@%s", (name))
#define ID_AS_ATTR(id) ATTR(queryEventAttributeName(id))
#define ELEM(id) queryEventName(id)

bool CPTreeEventVisitor::visitFile(const char* filename, uint32_t version)
{
    tree.setown(createPTree(EVT_PTREE_ROOT));
    tree->addProp(ATTR(EVT_PTREE_FILE_NAME), filename);
    active = tree->addPropTree(EVT_PTREE_HEADER, createPTree());
    active->addPropInt64(ATTR(EVT_PTREE_FILE_VERSION), version);
    return true;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitEvent(EventType id)
{
    active = tree->addPropTree(EVT_PTREE_EVENT, createPTree());
    active->addProp(ATTR(EVT_PTREE_EVENT_NAME), queryEventName(id));
    active->addPropInt(ATTR(EVT_PTREE_EVENT_ID), id);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id)
{
    ///active->addProp(ID_AS_ATTR(id), "");
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, const char* value)
{
    if (EvAttrSysOption == id)
        active->addPropBool(VStringBuffer("@%s", value), true);
    else
        active->addProp(ID_AS_ATTR(id), value);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, bool value)
{
    active->addPropBool(ID_AS_ATTR(id), value);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, uint8_t value)
{
    active->addPropInt(ID_AS_ATTR(id), value);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, uint16_t value)
{
    active->addPropInt(ID_AS_ATTR(id), value);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, uint32_t value)
{
    active->addPropInt64(ID_AS_ATTR(id), value);
    return visitContinue;
}

IEventVisitor::Continuation CPTreeEventVisitor::visitAttribute(EventAttr id, uint64_t value)
{
    active->addPropInt64(ID_AS_ATTR(id), (int64_t)value);
    return visitContinue;
}

void CPTreeEventVisitor::leaveFile(uint32_t bytesRead)
{
    active = tree->addPropTree(EVT_PTREE_FOOTER, createPTree());
    active->addPropInt64(ATTR(EVT_PTREE_FILE_BYTES_READ), bytesRead);
}

CPTreeEventVisitor::CPTreeEventVisitor(std::ostream& _out)
    : out(_out)
{
}

#undef ELEM
#undef ID_AS_ATTR
