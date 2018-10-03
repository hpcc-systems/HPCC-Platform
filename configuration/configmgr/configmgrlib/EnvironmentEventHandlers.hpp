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


#ifndef _CONFIG2_CONFIGEVENTS_HPP_
#define _CONFIG2_CONFIGEVENTS_HPP_

#include <string>
#include <map>
#include <memory>
#include <vector>
#include "NameValue.hpp"

class EnvironmentNode;

class EnvironmentEventHandler
{
    public:

        EnvironmentEventHandler(const std::string &type) : m_eventType(type) {}
        virtual ~EnvironmentEventHandler() {}
        virtual void processEvent(const std::string &eventType, std::shared_ptr<EnvironmentNode> pEventNode) = 0;
        virtual void doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode) = 0;


    protected:

        std::string m_eventType;
};


class MatchEnvironmentEventHandler : public EnvironmentEventHandler
{
    public:

        MatchEnvironmentEventHandler() : EnvironmentEventHandler("create") {}
        virtual ~MatchEnvironmentEventHandler() {}
        void setItemType(const std::string &type) { m_itemType = type; }
        void setEventNodeAttributeName(const std::string &name);
        void setTargetAttributeName(const std::string &name) { m_targetAttribute = name; }
        void setTargetPath(const std::string &path) { m_targetPath = path; }

        virtual void processEvent(const std::string &eventType, std::shared_ptr<EnvironmentNode> pEventNode);


    protected:

        std::string m_itemType;
        std::string m_targetPath;
        std::string m_eventNodeAttribute;
        std::string m_targetAttribute;
};


class AttributeDependencyCreateEventHandler : public MatchEnvironmentEventHandler
{
    public:

        AttributeDependencyCreateEventHandler() {}
        virtual ~AttributeDependencyCreateEventHandler() {}
        void addDependency(const std::string &attrName, const std::string &attrValr, const std::string &depAttr, const std::string &depVal);
        virtual void doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode);


    protected:

        std::map<std::string, std::map<std::string, std::pair<std::string, std::string>>> m_depAttrVals;
};


class InsertEnvironmentDataCreateEventHandler : public MatchEnvironmentEventHandler
{
    public:

        InsertEnvironmentDataCreateEventHandler() {}
        virtual ~InsertEnvironmentDataCreateEventHandler() {}
        void setEnvironmentInsertData(const std::string &envData) { m_envData = envData;  }
        virtual void doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode);


    protected:

        std::string m_envData;
};


class AttributeSetValueCreateEventHandler : public MatchEnvironmentEventHandler
{
    public:

        AttributeSetValueCreateEventHandler() {}
        virtual ~AttributeSetValueCreateEventHandler() {}
        void addAttributeValue(const std::string &attrName, const std::string &attrValr);
        virtual void doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode);


    protected:

        std::vector<NameValue> m_attrVals;
};

#endif
