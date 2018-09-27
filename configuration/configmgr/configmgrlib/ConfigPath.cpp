/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "ConfigPath.hpp"
#include "Exceptions.hpp"
#include "Utils.hpp"


bool ConfigPathItem::checkValueAgainstValueList(const std::string val, bool returnTrueIfValueListEmpty) const
{
    bool found = false;

    if (m_attributeValues.empty())
    {
        return returnTrueIfValueListEmpty;
    }

    for (auto it=m_attributeValues.begin(); it!=m_attributeValues.end() && !found; ++it)
    {
        found = (*it == val);
    }
    return found == m_presentInList;
}


std::shared_ptr<ConfigPathItem> ConfigPath::getNextPathItem()
{
    std::shared_ptr<ConfigPathItem> pPathItem;

    //
    // If notheing left, return an empty pointer
    if (m_path.empty())
    {
        return pPathItem;
    }

    //
    // There is more to do, allocate a pointer and get to work
    pPathItem = std::make_shared<ConfigPathItem>();

    //
    // Root
    if (m_path[0] == '/')
    {
        parsePathElement(1, pPathItem);
        pPathItem->setIsRoot(true);
        updatePath(1);
    }

    //
    // Parent (MUST be done before current element test)
    else if (m_path.substr(0,2) == "..")
    {
        pPathItem->setIsParentPathItemn(true);
        updatePath(2);
    }

    //
    // Current element?
    else if (m_path[0] == '.')
    {
        pPathItem->setIsCurrentPathItem(true);
        updatePath(1);
    }

    //
    // Otherwise, need to parse it out
    else
    {
        parsePathElement(0, pPathItem);
        updatePath(0);
    }

    return pPathItem;
}


void ConfigPath::parsePathElement(std::size_t start, const std::shared_ptr<ConfigPathItem> &pPathItem)
{
    std::size_t slashPos = m_path.find_first_of('/', start);
    std::size_t len = (slashPos != std::string::npos) ? (slashPos-start) : std::string::npos;
    std::string element = m_path.substr(start, len);

    //
    // The attribute definition is enclosed by the brackets, extract it if present
    std::string attr;
    if (extractEnclosedString(element, attr, '[', ']', true))
    {
        //
        // Make sure valid
        if (attr.empty())
        {
            throw(ParseException("Bad path, missng attribute definition at or around: " + element));
        }

        //
        // The attribute must begin with a '@' or '#'. The '#' is an extension for config manager that allows the selection
        // of elements based on schema value properties as opposed to attribute values. So, for a '#', the name is that of a
        // schema item property and not an attribute in the environment. Useful for selecting all elements of a particular
        // type, for example, when elements have nothing in the environment that can be used to determine type such as ecl watch.
        if (attr[0] == '@' || attr[0] == '#')
        {
            pPathItem->setIsSchemaItem(attr[0] == '#');

            std::size_t notEqualPos = attr.find("!=");
            std::size_t equalPos = attr.find_first_of('=');
            std::size_t comparePos = (notEqualPos != std::string::npos) ? notEqualPos : equalPos;

            if (comparePos != std::string::npos)
            {
                pPathItem->setAttributeName(attr.substr(1, comparePos-1));
                std::size_t valPos = attr.find_first_of("('", comparePos);
                if (valPos != std::string::npos)
                {
                    std::string valueStr;
                    extractEnclosedString(attr.substr(valPos), valueStr, '(', ')', true);
                    std::vector<std::string> values = splitString(valueStr, ",");
                    for (auto &valstr : values)
                    {
                        std::string value;
                        extractEnclosedString(valstr, value, '\'', '\'', false);
                        pPathItem->addAttributeValue(value);
                    }
                    pPathItem->setExcludeValueList(notEqualPos != std::string::npos);
                }
                else
                {
                    throw(ParseException("Bad path, missng attribute values at or around: " + element));
                }
            }
            else
            {
                pPathItem->setAttributeName(attr.substr(1));
            }
        }
        else
        {
            throw(ParseException("Bad attribute, expected '@' or '#' at or around: " + element));
        }

        std::size_t bracketPos = element.find_first_of('[');
        pPathItem->setElementName(element.substr(0, bracketPos));
    }
    else
    {
        pPathItem->setElementName(element);
    }
}


void ConfigPath::updatePath(std::size_t startPos)
{
    std::size_t nextPos = m_path.find_first_of('/', startPos);
    if (nextPos != std::string::npos)
    {
        m_path = m_path.substr(nextPos+1);
    }
    else
    {
        m_path.clear();
    }
}
