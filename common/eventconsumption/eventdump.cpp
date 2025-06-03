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

void CDumpEventsOp::setFormat(OutputFormat _format)
{
    format = _format;
}

bool CDumpEventsOp::doOp()
{
    Owned<IEventAttributeVisitor> visitor;
    switch (format)
    {
    case OutputFormat::json:
        visitor.setown(createDumpJSONEventVisitor(*out));
        break;
    case OutputFormat::text:
        visitor.setown(createDumpTextEventVisitor(*out));
        break;
    case OutputFormat::xml:
        visitor.setown(createDumpXMLEventVisitor(*out));
        break;
    case OutputFormat::yaml:
        visitor.setown(createDumpYAMLEventVisitor(*out));
        break;
    case OutputFormat::csv:
        visitor.setown(createDumpCSVEventVisitor(*out));
        break;
    case OutputFormat::tree:
        {
            Owned<IEventPTreeCreator> creator = createEventPTreeCreator();
            if (traverseEvents(inputPath.str(), creator->queryVisitor()))
            {
                StringBuffer yaml;
                toYAML(creator->queryTree(), yaml, 2, 0);
                out->put(yaml.length(), yaml.str());
                out->put(1, "\n");
                return true;
            }
            return false;
        }
    default:
        throw makeStringExceptionV(-1, "unsupported output format: %d", (int)format);
    }
    return traverseEvents(inputPath.str(), *visitor);
}
