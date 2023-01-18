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

#include "EnvModTemplate.hpp"
#include "rapidjson/error/en.h"
#include "TemplateException.hpp"
#include "TemplateExecutionException.hpp"
#include "OperationCreateNode.hpp"
#include "OperationFindNode.hpp"
#include "OperationModifyNode.hpp"
#include "OperationDeleteNode.hpp"
#include <sstream>
#include <fstream>
#include "Utils.hpp"

#ifdef _MSC_VER
#undef GetObject
#endif

//
// Good online resource for validation of modification templates is https://www.jsonschemavalidator.net/
// Load the schecma (ModTemplateSchema.json) in the left window and the modification template in the right.

EnvModTemplate::EnvModTemplate(EnvironmentMgr *pEnvMgr, const std::string &schemaFile) : m_pEnvMgr(pEnvMgr), m_pTemplate(nullptr), m_pSchema(nullptr)
{
    if (m_pEnvMgr == nullptr)
    {
        throw(TemplateException("Environment Modification Template requires a valid Environment Manager"));
    }

    //
    // Load and compile the schema document
    std::ifstream jsonFile(schemaFile);
    rapidjson::IStreamWrapper jsonStream(jsonFile);
    rapidjson::Document sd;
    if (sd.ParseStream(jsonStream).HasParseError())
    {
        throw(TemplateException(&sd));
    }
    m_pSchema = new rapidjson::SchemaDocument(sd); // Compile a Document to SchemaDocument
}


EnvModTemplate::~EnvModTemplate()
{
    releaseTemplate();
    if (m_pSchema != nullptr)
    {
        delete m_pSchema;
        m_pSchema = nullptr;
    }
}


void EnvModTemplate::releaseTemplate()
{
    if (m_pTemplate != nullptr)
    {
        delete m_pTemplate;
        m_pTemplate = nullptr;
    }
}



void EnvModTemplate::loadTemplateFromFile(const std::string &fqTemplateFile)
{
    std::ifstream jsonFile(fqTemplateFile);
    rapidjson::IStreamWrapper jsonStream(jsonFile);
    loadTemplate(jsonStream);
}


void EnvModTemplate::loadTemplateFromJson(const std::string &templateJson)
{
    std::stringstream json;
    json << templateJson;
    rapidjson::IStreamWrapper jsonStream(json);
    loadTemplate(jsonStream);
}


void EnvModTemplate::loadTemplate(rapidjson::IStreamWrapper &stream)
{
    //
    // Cleanup anything that may be laying around.
    releaseTemplate();
    m_variables.clear();

    m_pTemplate = new rapidjson::Document();

    //
    // Parse and make sure it's valid JSON
    m_pTemplate->ParseStream(stream);
    if (m_pTemplate->HasParseError())
    {
        throw(TemplateException(m_pTemplate));
    }

    if (m_pSchema != nullptr)
    {
        rapidjson::SchemaValidator validator(*m_pSchema);
        if (!m_pTemplate->Accept(validator))
        {
            // Input JSON is invalid according to the schema
            // Output diagnostic information
            std::string msg = "Template failed validation, ";
            rapidjson::StringBuffer sb;
            validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
            msg += " Invalid schema: " + std::string(sb.GetString()) + ", ";
            msg += " Invalid keyword: " + std::string(validator.GetInvalidSchemaKeyword()) + ", ";
            sb.Clear();
            validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
            msg += "Invalid document: " + std::string(sb.GetString());
            throw TemplateException(msg, true);
        }
    }

    //
    // Now, parse the template, all as part of loading it
    parseTemplate();
}


void EnvModTemplate::parseTemplate()
{
    //
    // Process the defined template sections. If any errors are found, thrown an exception

    try
    {
        //
        // Inputs
        if (m_pTemplate->HasMember("variables"))
        {
            const rapidjson::Value &variables = (*m_pTemplate)["variables"];
            parseVariables(variables);
        }

        //
        // Operations (a required section)
        parseOperations((*m_pTemplate)["operations"]);
    }
    catch (TemplateException &te)
    {
        throw;  // just rethrow
    }
    catch (...)
    {
        throw TemplateException("An exception has occured while parsing the input template", false);
    }
}


void EnvModTemplate::parseVariables(const rapidjson::Value &variables)
{
    for (auto &varDef: variables.GetArray())
    {
        parseVariable(varDef);
    }
}


void EnvModTemplate::parseVariable(const rapidjson::Value &varValue)
{
    rapidjson::Value::ConstMemberIterator it;
    std::shared_ptr<Variable> pVariable;

    //
    // input name, make sure not a duplicate
    std::string varName(varValue.FindMember("name")->value.GetString());
    std::string type(varValue.FindMember("type")->value.GetString());
    pVariable = variableFactory(type, varName);

    //
    // Get and set the rest of the input values
    it = varValue.FindMember("prompt");
    if (it != varValue.MemberEnd())
        pVariable->m_userPrompt = it->value.GetString();
    else
        pVariable->m_userPrompt = "Input value for " + varName;

    it = varValue.FindMember("description");
    if (it != varValue.MemberEnd())
    {
        pVariable->m_description = it->value.GetString();
    }

    it = varValue.FindMember("values");
    if (it != varValue.MemberEnd())
    {
        for (auto &val: it->value.GetArray())
        {
            pVariable->addValue(trim(val.GetString()));
        }
    }

    it = varValue.FindMember("prepared_value");
    if (it != varValue.MemberEnd())
    {
        pVariable->m_preparedValue = trim(it->value.GetString());
    }

    it = varValue.FindMember("user_input");
    if (it != varValue.MemberEnd())
    {
        pVariable->m_userInput = it->value.GetBool();
    }

    m_variables.add(pVariable);
}


std::shared_ptr<Variable> EnvModTemplate::getVariable(const std::string &name, bool throwIfNotFound) const
{
    return m_variables.getVariable(name, throwIfNotFound);
}


std::vector<std::shared_ptr<Variable>> EnvModTemplate::getVariables(bool userInputOnly) const
{
    std::vector<std::shared_ptr<Variable>> variables;
    if (!userInputOnly)
    {
        variables.insert( variables.end(), m_variables.all().begin(), m_variables.all().end() );
    }
    else
    {
        auto allVars = m_variables.all();
        for (auto &varIt: allVars)
        {
            if (varIt->m_userInput)
                variables.emplace_back(varIt);
        }
    }

    return variables;
}


void EnvModTemplate::assignVariablesFromFile(const std::string &filepath)
{
    std::ifstream jsonFile(filepath);
    rapidjson::IStreamWrapper jsonStream(jsonFile);

    //
    // Format:
    // {
    //   "variable-name" : [ "value1", "value2" , ..., "valuen"],
    //     ...
    // }

    rapidjson::Document inputJson;

    //
    // Parse and make sure it's valid JSON
    inputJson.ParseStream(jsonStream);
    if (inputJson.HasParseError())
    {
        throw(TemplateException(&inputJson));
    }

    //
    // go through all the inputs and assign values
    for (auto itr = inputJson.MemberBegin();
         itr != inputJson.MemberEnd(); ++itr)
    {
        std::string inputName = itr->name.GetString();
        auto pInput = m_variables.getVariable(inputName);
        auto valueArray = itr->value.GetArray();
        for (auto &val: valueArray)
        {
            pInput->addValue(val.GetString());
        }
    }
}


void EnvModTemplate::parseOperations(const rapidjson::Value &operations)
{
    for (auto &opDef: operations.GetArray())
    {
        parseOperation(opDef);
    }
}


void EnvModTemplate::parseOperation(const rapidjson::Value &operation)
{
    std::shared_ptr<Operation> pOp;
    std::string action(operation.FindMember("action")->value.GetString());

    if (action == "create")
    {
        pOp = std::make_shared<OperationCreateNode>();
    }
    else if (action == "modify")
    {
        pOp = std::make_shared<OperationModifyNode>();
    }
    else if (action == "find")
    {
        pOp = std::make_shared<OperationFindNode>();
    }
    else if (action == "delete")
    {
        pOp = std::make_shared<OperationDeleteNode>();
    }
    else
    {
        throw TemplateException("Unsupported operation '" + action + "' found", false);
    }

    //
    // Get the parent based on presence of key (schema validates presence of one of these)
    auto it = operation.FindMember("target_path");
    if (it != operation.MemberEnd())
    {
        pOp->m_path = trim(it->value.GetString());
    }
    else
    {
        it = operation.FindMember("target_nodeid");
        pOp->m_parentNodeId = trim(it->value.GetString());
    }

    //
    // Parse the data section
    auto dataIt = operation.FindMember("data");
    if (dataIt != operation.MemberEnd())
    {
        parseOperationCommonData(dataIt->value, pOp);

        //
        // Parse specific operation type values
        if (action == "create")
        {
            std::shared_ptr<OperationCreateNode> pCreateOp = std::dynamic_pointer_cast<OperationCreateNode>(pOp);
            pCreateOp->m_nodeType = dataIt->value.FindMember("node_type")->value.GetString();
        }
        else if (action == "find")
        {
            std::shared_ptr<OperationFindNode> pFindOp = std::dynamic_pointer_cast<OperationFindNode>(pOp);
            it = dataIt->value.FindMember("create_if_not_found");
            if (it != dataIt->value.MemberEnd())
            {
                pFindOp->m_createIfNotFound = it->value.GetBool();
            }

            it = dataIt->value.FindMember("node_type");
            if (it != dataIt->value.MemberEnd())
            {
                pFindOp->m_nodeType = dataIt->value.FindMember("node_type")->value.GetString();
            }
        }
    }

    m_operations.emplace_back(pOp);
}


void EnvModTemplate::parseOperationCommonData(const rapidjson::Value &operationData, std::shared_ptr<Operation> pOp)
{
    rapidjson::Value::ConstMemberIterator it;
    auto dataObj = operationData.GetObject();


    //
    // Get the count (optional, default is 1)
    it = dataObj.FindMember("count");
    if (it != dataObj.MemberEnd())
        pOp->m_count = trim(it->value.GetString());

    //
    // Get the starting index (optional, for windowing into a range of values)
    it = dataObj.FindMember("start_index");
    if (it != dataObj.MemberEnd())
        pOp->m_startIndex = trim(it->value.GetString());

    //
    // Save node id
    rapidjson::Value::ConstMemberIterator saveInfoIt = dataObj.FindMember("save");
    if (saveInfoIt != dataObj.MemberEnd())
    {
        pOp->m_saveNodeIdName = saveInfoIt->value.GetObject().FindMember("name")->value.GetString();
        it = saveInfoIt->value.GetObject().FindMember("duplicate_ok");
        if (it != saveInfoIt->value.MemberEnd()) pOp->m_duplicateSaveNodeIdInputOk = it->value.GetBool();
    }

    //
    // Node value?
    rapidjson::Value::ConstMemberIterator nodeValueIt = dataObj.FindMember("node_value");
    if (nodeValueIt != dataObj.MemberEnd())
    {
        parseAttribute(nodeValueIt->value, &pOp->m_nodeValue);
        pOp->m_nodeValueValid = true;
    }

    //
    // Get the attributes. Note that the allowed key/value pairs for each entry in this array vary by operation type.
    // The schema validates that the member objects are correct based on the operation type.
    it = dataObj.FindMember("attributes");
    if (it != dataObj.MemberEnd())
    {
        rapidjson::Value::ConstMemberIterator attrValueIt;
        std::string attrName, attrValue;
        for (auto &attr: it->value.GetArray())
        {
            modAttribute newAttribute;
            parseAttribute(attr, &newAttribute);
            pOp->addAttribute(newAttribute);
        }
    }

    //
    // Throw on empty is a flag that will force an exception to be thrown during execution if no
    // nodes are found to modify.
    it = dataObj.FindMember("error_if_not_found");
    if (it != dataObj.MemberEnd())
        pOp->m_throwOnEmpty = it->value.GetBool();
}


void EnvModTemplate::parseAttribute(const rapidjson::Value &attributeValue, modAttribute *pAttribute)
{
    auto attributeData = attributeValue.GetObject();
    //
    // Get the attribute name or set of names. One shall be present based on the schema and operation type
    auto valueIt = attributeData.FindMember("name");
    if (valueIt != attributeData.MemberEnd())
    {
        pAttribute->addName(trim(valueIt->value.GetString()));
    }
    else
    {
        valueIt = attributeData.FindMember("first_of");
        if (valueIt != attributeData.MemberEnd())
        {
            for (auto &nameIt: valueIt->value.GetArray())
            {
                pAttribute->addName(trim(nameIt.GetString()));
            }
        }
    }

    //
    // Remaining attribute values and settings
    valueIt = attributeData.FindMember("value");
    if (valueIt != attributeData.MemberEnd())
    {
        pAttribute->value = trim(valueIt->value.GetString());
    }

    auto attrValueIt = attributeData.FindMember("start_index");
    if (attrValueIt != attributeData.MemberEnd())
    {
        pAttribute->startIndex = trim(attrValueIt->value.GetString());
    }

    attrValueIt = attributeData.FindMember("do_not_set");
    if (attrValueIt != attributeData.MemberEnd())
    {
        pAttribute->doNotSet = attrValueIt->value.GetBool();
    }

    auto saveInfoIt = attributeData.FindMember("save");
    if (saveInfoIt != attributeData.MemberEnd())
    {
        pAttribute->saveVariableName = saveInfoIt->value.GetObject().FindMember("name")->value.GetString();
        attrValueIt = saveInfoIt->value.GetObject().FindMember("duplicate_ok");
        if (attrValueIt != saveInfoIt->value.MemberEnd())
            pAttribute->duplicateSaveValueOk = attrValueIt->value.GetBool();
    }

    attrValueIt = attributeData.FindMember("error_if_not_found");
    if (attrValueIt != attributeData.MemberEnd())
    {
        pAttribute->errorIfNotFound = attrValueIt->value.GetBool();
    }

    attrValueIt = attributeData.FindMember("error_if_empty");
    if (attrValueIt != attributeData.MemberEnd())
    {
        pAttribute->errorIfEmpty = attrValueIt->value.GetBool();
    }
}


void EnvModTemplate::execute()
{
    try
    {
        //
        // Do final prep on inputs. This may set values for inputs that are dependent on previously set inputs
        m_variables.prepare();
    }
    catch (TemplateExecutionException &te)
    {
        te.setStep("Variable preparation");
        throw;
    }

    unsigned opNum = 1;
    for (auto &pOp: m_operations)
    {
        try
        {
            pOp->execute(m_pEnvMgr, &m_variables);
        }
        catch (TemplateExecutionException &te)
        {
            //
            // Set the operation step number and rethrow so user can tell what step caused the problem
            te.setStep("Operation step " + std::to_string(opNum));
            throw;
        }
        ++opNum;
    }
}
