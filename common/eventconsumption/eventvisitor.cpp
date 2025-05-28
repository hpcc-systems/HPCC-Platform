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

#include "eventvisitor.h"
#include "jiface.hpp"
#include "jexcept.hpp"

bool eventDistributor(IEvent& event, IEventAttributeVisitor& visitor)
{
    IEventAttributeVisitor::Continuation result = visitor.visitEvent(event.queryType());
    switch (result)
    {
    case IEventAttributeVisitor::visitSkipEvent:
        return true;
    case IEventAttributeVisitor::visitSkipFile:
        return false;
    }
    Owned<IEventAttributeIterator> attributes = event.getAttributes();
    ForEach(*attributes)
    {
        const IEventAttribute& attribute = attributes->query();
        if (attribute.isNumeric())
            result = visitor.visitAttribute(attribute.queryId(), attribute.queryNumericValue());
        else if (attribute.isText())
            result = visitor.visitAttribute(attribute.queryId(), attribute.queryTextValue());
        else if (attribute.isBoolean())
            result = visitor.visitAttribute(attribute.queryId(), attribute.queryBooleanValue());
        else
            throw makeStringException(-1, "unknown attribute type");
        switch (result)
        {
        case IEventAttributeVisitor::visitSkipEvent:
            return true;
        case IEventAttributeVisitor::visitSkipFile:
            return false;
        }
    }
    return visitor.departEvent();
}
