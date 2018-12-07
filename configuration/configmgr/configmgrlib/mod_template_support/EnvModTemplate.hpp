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
#include "Input.hpp"
#include "Inputs.hpp"
#include "Operation.hpp"
#include <map>
#include <vector>


class EnvModTemplate
{

    public:

        EnvModTemplate(EnvironmentMgr *pEnvMgr, const std::string &fqSchemaFile);
        ~EnvModTemplate();

        void loadTemplateFromJson(const std::string &templateJson);
        void loadTemplateFromFile(const std::string &fqTemplateFile);
        std::shared_ptr<Input> getInput(const std::string &name, bool throwIfNotFound = true) const;
        std::vector<std::shared_ptr<Input>> getInputs(int phase = 0) const;
        bool execute();


    protected:

        void releaseTemplate();
        void loadTemplate(rapidjson::IStreamWrapper &stream);
        void parseTemplate();
        void parseInputs(const rapidjson::Value &inputs);
        void parseInput(const rapidjson::Value &input);
        void parseOperations(const rapidjson::Value &operations);
        void parseOperation(const rapidjson::Value &operation);
        void parseOperationCommon(const rapidjson::Value &operation, std::shared_ptr<Operation> pOp);
        std::shared_ptr<Operation> parseCreateNodeOperation(const rapidjson::Value &operation);
        std::shared_ptr<Operation> parseOperationNoop(const rapidjson::Value &operation);


    protected:

        rapidjson::SchemaDocument *m_pSchema;
        rapidjson::Document *m_pTemplate;   // same as GenericDocument<UTF8<> >
        EnvironmentMgr *m_pEnvMgr;
        Inputs m_inputs;
        std::vector<std::shared_ptr<Operation>> m_operations;
};


#endif //HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATE_HPP
