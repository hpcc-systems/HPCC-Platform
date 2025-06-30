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

#include "eventiterator.h"

bool CPropertyTreeEvents::nextEvent(CEvent& event)
{
    if (eventsIt->isValid())
    {
        const IPropertyTree& node = eventsIt->query();
        const char* typeStr = node.queryProp("@type");
        EventType type = queryEventType(typeStr);
        if (EventNone == type)
            throw makeStringExceptionV(-1, "unknown event type: %s", typeStr);
        event.reset(type);

        // accept all attributes defined for the event type
        for (CEventAttribute& attr : event.definedAttributes)
        {
            VStringBuffer xpath("@%s", queryEventAttributeName(attr.queryId()));
            const char* valueStr = node.queryProp(xpath);
            if (isEmptyString(valueStr))
                continue;
            switch (attr.queryTypeClass())
            {
            case EATCtext:
            case EATCtimestamp:
                attr.setValue(valueStr);
                break;
            case EATCnumeric:
                attr.setValue(strtoull(valueStr, nullptr, 0));
                break;
            case EATCboolean:
                attr.setValue(strToBool(valueStr));
                break;
            default:
                throw makeStringExceptionV(-1, "unknown attribute type class %u for %s/%s", attr.queryTypeClass(), typeStr, xpath.str());
            }
        }

        // advance to the next matching node
        (void)eventsIt->next();

        // the requested event was found
        return true;
    }
    return false;
}

CPropertyTreeEvents::CPropertyTreeEvents(const IPropertyTree& events)
    : eventsIt(events.getElements("event"))
{
    // enable the "next" event to populate from the first matching node
    (void)eventsIt->first();
}
