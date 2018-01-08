/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems�.

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


//
// Todo:
//   Add a required if capability. hpp:requiredIf="some indicator to a value that if set means this one must be set"
//   Maybe a component name? Although I think displayName should suffice
//   See if can put same ESP Service in ESP Process more than once.


#ifndef _CONFIG2_ENVIRONMENTMGR_HPP_
#define _CONFIG2_ENVIRONMENTMGR_HPP_

#include <string>
#include <fstream>
#include <vector>
#include "SchemaItem.hpp"
#include "SchemaParser.hpp"
#include "EnvironmentNode.hpp"
#include "Status.hpp"

class EnvironmentMgr;

EnvironmentMgr *getEnvironmentMgrInstance(const std::string &envType);


class EnvironmentMgr
{
    public:

        struct valueDef {
            std::string name;
            std::string value;
        };

        EnvironmentMgr();
        virtual ~EnvironmentMgr() { }

        // add a load from stream?
        bool loadSchema(const std::string &configPath, const std::string &masterConfigFile, const std::vector<std::string> &cfgParms = std::vector<std::string>());
        std::string getLastSchemaMessage() const;
        std::string getLastEnvironmentMessage() const { return m_message;  }
        bool loadEnvironment(const std::string &file);

        std::shared_ptr<EnvironmentNode> getEnvironmentNode(const std::string &nodeId);
        std::shared_ptr<EnvironmentNode> addNewEnvironmentNode(const std::string &parentNodeId, const std::string &elementType, Status &status);
        std::shared_ptr<EnvironmentNode> addNewEnvironmentNode(const std::shared_ptr<EnvironmentNode> &pParentNode, const std::shared_ptr<SchemaItem> &pNewCfgItem, Status &status);
        bool removeEnvironmentNode(const std::string &nodeId, Status &status);
        
        void saveEnvironment(const std::string &file);
        void validate(Status &status) const;


    protected:

        std::string getUniqueKey();
        
        void addPath(const std::shared_ptr<EnvironmentNode> pNode);
        virtual bool createParser(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms) = 0;
        virtual bool doLoadEnvironment(std::istream &in) = 0;
        virtual void save(std::ostream &out) = 0;


    protected:

        std::shared_ptr<SchemaItem> m_pSchema;
        std::shared_ptr<SchemaParser> m_pSchemaParser;
        std::shared_ptr<EnvironmentNode> m_pRootNode;
        std::map<std::string, std::shared_ptr<EnvironmentNode>> m_nodeIds;
        std::string m_message;


    private:
        
        int m_key;
};

#endif