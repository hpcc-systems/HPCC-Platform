/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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


#ifndef _CONFIG2_ENVIRONMENTMGR_HPP_
#define _CONFIG2_ENVIRONMENTMGR_HPP_

#include <string>
#include <fstream>
#include <vector>
#include <atomic>
#include "SchemaItem.hpp"
#include "SchemaParser.hpp"
#include "EnvironmentNode.hpp"
#include "Status.hpp"
#include "NameValue.hpp"
#include "platform.h"

class EnvironmentMgr;

enum EnvironmentType
{
    UNDEFINED,
    XML
};


DECL_EXPORT EnvironmentMgr *getEnvironmentMgrInstance(const EnvironmentType envType);

class DECL_EXPORT EnvironmentMgr
{
    public:

        EnvironmentMgr();
        virtual ~EnvironmentMgr() { }
        bool loadSchema(const std::string &configPath, const std::string &masterConfigFile, const std::vector<std::string> &cfgParms = std::vector<std::string>());
        std::string getLastSchemaMessage() const;
        std::string getLastEnvironmentMessage() const { return m_message;  }
        bool loadEnvironment(const std::string &qualifiedFilename);
        std::shared_ptr<EnvironmentNode> getEnvironmentNode(const std::string &nodeId);
        std::shared_ptr<EnvironmentNode> addNewEnvironmentNode(const std::string &parentNodeId, const std::string &configType, Status &status);
        bool removeEnvironmentNode(const std::string &nodeId);
        bool saveEnvironment(const std::string &qualifiedFilename);
        void discardEnvironment() { m_pRootNode = nullptr; m_nodeIds.clear(); m_key=1; }
        void validate(Status &status, bool includeHiddenNodes=false) const;


    protected:

        std::string getUniqueKey();
        void addPath(const std::shared_ptr<EnvironmentNode> pNode);
        virtual bool createParser() = 0;
        std::shared_ptr<EnvironmentNode> addNewEnvironmentNode(const std::shared_ptr<EnvironmentNode> &pParentNode, const std::shared_ptr<SchemaItem> &pNewCfgItem, Status &status);
        virtual bool doLoadEnvironment(std::istream &in) = 0;
        virtual bool save(std::ostream &out) = 0;


    protected:

        std::shared_ptr<SchemaItem> m_pSchema;
        std::shared_ptr<SchemaParser> m_pSchemaParser;
        std::shared_ptr<EnvironmentNode> m_pRootNode;
        std::map<std::string, std::shared_ptr<EnvironmentNode>> m_nodeIds;
        std::string m_message;


    private:

        std::atomic_int m_key;
};

#endif
