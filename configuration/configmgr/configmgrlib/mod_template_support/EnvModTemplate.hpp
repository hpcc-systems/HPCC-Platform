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

#ifndef HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATE_HPP
#define HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATE_HPP

#include "EnvironmentMgr.hpp"
#include "rapidjson/document.h"
#include "rapidjson/schema.h"
#include "rapidjson/istreamwrapper.h"
#include "TemplateException.hpp"
#include <string>
#include "Variable.hpp"
#include "Variables.hpp"
#include "Operation.hpp"
#include "OperationFindNode.hpp"
#include <map>
#include <vector>


class EnvModTemplate
{

    public:

        EnvModTemplate(EnvironmentMgr *pEnvMgr, const std::string &schemaFile);
        ~EnvModTemplate();

        void loadTemplateFromJson(const std::string &templateJson);
        void loadTemplateFromFile(const std::string &fqTemplateFile);
        std::shared_ptr<Variable> getVariable(const std::string &name, bool throwIfNotFound = true) const;
        std::vector<std::shared_ptr<Variable>> getVariables(bool userInputOnly = false) const;
        void assignVariablesFromFile(const std::string &filepath);
        void execute();


    protected:

        void releaseTemplate();
        void loadTemplate(rapidjson::IStreamWrapper &stream);
        void parseTemplate();
        void parseVariables(const rapidjson::Value &variables);
        void parseVariable(const rapidjson::Value &varValue);
        void parseOperations(const rapidjson::Value &operations);
        void parseOperation(const rapidjson::Value &operation);
        void parseOperationCommonData(const rapidjson::Value &operationData, std::shared_ptr<Operation> pOp);
        void parseOperationFindAttributes(const rapidjson::Value &operationData, std::shared_ptr<OperationFindNode> pFindOp);


    protected:

        rapidjson::SchemaDocument *m_pSchema;
        rapidjson::Document *m_pTemplate;   // same as GenericDocument<UTF8<> >
        EnvironmentMgr *m_pEnvMgr;
        Variables m_variables;
        std::vector<std::shared_ptr<Operation>> m_operations;
};


#endif //HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATE_HPP
