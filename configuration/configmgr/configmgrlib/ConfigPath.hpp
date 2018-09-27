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


#ifndef _CONFIG2_CONFIGPATH_HPP_
#define _CONFIG2_CONFIGPATH_HPP_

#include <string>
#include <vector>
#include <memory>

class ConfigPathItem
{
    public:

        ConfigPathItem() : m_isParentPathItem(false), m_isCurrentPathItem(false), m_isSchemaItem(false), m_isRoot(false), m_presentInList(true) {}
        ConfigPathItem(const std::string &elemName) : m_isParentPathItem(false), m_isCurrentPathItem(false), m_isSchemaItem(false), m_isRoot(false), m_elementName(elemName) {}

        void setElementName(const std::string &elemName) { m_elementName = elemName; }
        const std::string &getElementName() const { return m_elementName; }
        void setAttributeName(const std::string &attrName) { m_attributeName = attrName; }
        const std::string &getAttributeName() const { return m_attributeName; }
        void addAttributeValue(const std::string &attrValue) { m_attributeValues.push_back(attrValue); }
        bool hasAttributeValues() const { return m_attributeValues.size() > 0; }
        std::vector<std::string> getAttributeValues() const { return m_attributeValues; }
        bool checkValueAgainstValueList(const std::string val, bool returnTrueIfValueListEmpty = true) const;
        void setIsCurrentPathItem(bool currentItem) { m_isCurrentPathItem = currentItem; }
        bool isCurrentPathItem() const { return m_isCurrentPathItem; }
        void setIsParentPathItemn(bool parentElement) { m_isParentPathItem = parentElement; }
        bool isParentPathtItem() const { return m_isParentPathItem; }
        void setIsSchemaItem(bool isSchema) { m_isSchemaItem = isSchema; }
        bool isSchemaItem() const { return m_isSchemaItem; }
        void setIsRoot(bool root) { m_isRoot = root; }
        bool isRoot() const { return m_isRoot; }
        void setExcludeValueList(bool exclude) { m_presentInList = !exclude; }


    private:

        bool m_isParentPathItem;
        bool m_isCurrentPathItem;
        bool m_isSchemaItem;
        bool m_isRoot;
        bool m_presentInList;
        std::string m_elementName;
        std::string m_attributeName;
        std::vector<std::string> m_attributeValues;
};



class ConfigPath
{
    public:

        ConfigPath(const std::string path) : m_path(path) {}
        ~ConfigPath() {}
        std::shared_ptr<ConfigPathItem> getNextPathItem();
        bool isPathRemaining() const { return (!m_path.empty()); }


    protected:

        void updatePath(std::size_t pos);
        void parsePathElement(std::size_t start, const std::shared_ptr<ConfigPathItem> &pPathItem);


    private:

        std::string m_path;
};


#endif
