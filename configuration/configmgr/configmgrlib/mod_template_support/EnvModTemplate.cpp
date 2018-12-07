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
#include "OperationCreateNode.hpp"
#include "OperationNoop.hpp"
#include <sstream>
#include <fstream>
#include "Utils.hpp"


EnvModTemplate::EnvModTemplate(EnvironmentMgr *pEnvMgr, const std::string &fqSchemaFile) : m_pEnvMgr(pEnvMgr), m_pTemplate(nullptr), m_pSchema(nullptr)
{
    if (m_pEnvMgr == nullptr)
    {
        throw(TemplateException("Environment Modification Template requires a valid Environment Manager to be initialized"));
    }

    //
    // Load and compile the schema document
    std::ifstream jsonFile(fqSchemaFile);
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
    m_inputs.clear();

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
            TemplateException te(msg);
            te.setInvalidTemplate(true);
            throw te;
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

    //
    // Inputs
    if (m_pTemplate->HasMember("inputs"))
    {
        const rapidjson::Value &inputs = (*m_pTemplate)["inputs"];
        parseInputs(inputs);
    }

    //
    // Operations (a required section)
    parseOperations((*m_pTemplate)["operations"]);
}


void EnvModTemplate::parseInputs(const rapidjson::Value &inputs)
{
    for (auto &inputDef: inputs.GetArray())
    {
        parseInput(inputDef);
    }
}


void EnvModTemplate::parseInput(const rapidjson::Value &input)
{
    rapidjson::Value::ConstMemberIterator it;
    std::shared_ptr<Input> pInput;

    //
    // input name, make sure not a duplicate
    std::string inputName(input.FindMember("name")->value.GetString());
    std::string type(input.FindMember("type")->value.GetString());
    pInput = inputValueFactory(type, inputName);

    //
    // Get and set the rest of the input values
    it = input.FindMember("prompt");
    if (it != input.MemberEnd())
        pInput->setUserPrompt(it->value.GetString());
    else
        pInput->setUserPrompt("Input value for " + inputName);

    it = input.FindMember("description");
    if (it != input.MemberEnd()) pInput->setDescription(it->value.GetString());

    it = input.FindMember("tooltip");
    if (it != input.MemberEnd()) pInput->setTooltip(it->value.GetString());

    it = input.FindMember("value");
    if (it != input.MemberEnd()) pInput->setValue(trim(it->value.GetString()));

    it = input.FindMember("prepared_value");
    if (it != input.MemberEnd()) pInput->setPreparedValue(trim(it->value.GetString()));

    m_inputs.add(pInput);
}


std::shared_ptr<Input> EnvModTemplate::getInput(const std::string &name, bool throwIfNotFound) const
{
    return m_inputs.getInput(name, throwIfNotFound);
}


std::vector<std::shared_ptr<Input>> EnvModTemplate::getInputs(int phase) const
{
    return m_inputs.all(phase);
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

    if (action == "create_node")
    {
        pOp = parseCreateNodeOperation(operation);
    }
    else if (action == "noop" || action == "modify_node")
    {
        pOp = parseOperationNoop(operation);
    }
    else
    {
        throw TemplateException("Unsupported operation '" + action + "' found", false);
    }

    m_operations.emplace_back(pOp);
}


void EnvModTemplate::parseOperationCommon(const rapidjson::Value &operation, std::shared_ptr<Operation> pOp)
{
    rapidjson::Value::ConstMemberIterator it;

    //
    // Either a node ID or a path
    it = operation.FindMember("parent_nodeid");
    if (it != operation.MemberEnd())
        pOp->setParentNodeId(it->value.GetString());
    else
        pOp->setPath(operation.FindMember("path")->value.GetString());

    //
    // Get the count
    it = operation.FindMember("count");
    if (it != operation.MemberEnd())
        pOp->setCount(trim(it->value.GetString()));

    //
    // Get the starting index (for windowing into a range of values)
    it = operation.FindMember("start_index");
    if (it != operation.MemberEnd())
        pOp->setStartIndex(trim(it->value.GetString()));

    //
    // Get the attributes (if any)
    it = operation.FindMember("attributes");
    if (it != operation.MemberEnd())
    {
        rapidjson::Value::ConstMemberIterator attrValueIt;
        std::string attrName, attrValue;
        for (auto &attr: it->value.GetArray())
        {
            //
            // Get the attribute name and value. Note that these are required by the schema so the template would
            // have never loaded if either was missing.
            attrName = trim(attr.FindMember("name")->value.GetString());
            attrValue = trim(attr.FindMember("value")->value.GetString());

            modAttribute newAttribute(attrName, attrValue);

            attrValueIt = attr.FindMember("start_index");
            if (attrValueIt != attr.MemberEnd()) newAttribute.startIndex = trim(attrValueIt->value.GetString());

            attrValueIt = attr.FindMember("do_not_set");
            if (attrValueIt != attr.MemberEnd()) newAttribute.doNotSet = attrValueIt->value.GetBool();

            attrValueIt = attr.FindMember("save_value");
            if (attrValueIt != attr.MemberEnd()) newAttribute.saveValue = trim(attrValueIt->value.GetString());


            pOp->addAttribute(newAttribute);
        }
    }
}


std::shared_ptr<Operation> EnvModTemplate::parseCreateNodeOperation(const rapidjson::Value &operation)
{
    rapidjson::Value::ConstMemberIterator it;
    std::shared_ptr<OperationCreateNode> pOp = std::make_shared<OperationCreateNode>();

    parseOperationCommon(operation, pOp);

    // set the node type (schema ensures its presence)
    pOp->setNodeType(operation.FindMember("node_type")->value.GetString());

    //
    // Get the potential name underwhich the newly created node ID will be saved
    it = operation.FindMember("save_nodeid");
    if (it != operation.MemberEnd())
        pOp->setSaveNodeIdName(it->value.GetString());

    return pOp;
}


std::shared_ptr<Operation> EnvModTemplate::parseOperationNoop(const rapidjson::Value &operation)
{
    rapidjson::Value::ConstMemberIterator it;
    std::shared_ptr<OperationNoop> pOp = std::make_shared<OperationNoop>();
    parseOperationCommon(operation, pOp);

    it = operation.FindMember("count");
    if (it != operation.MemberEnd())
        pOp->setCount(it->value.GetString());

    return pOp;
}


bool EnvModTemplate::execute()
{
    bool rc = false;

    //
    // Do final prep on inputs. This may set values for inputs that are dependent on previously set inputs
    m_inputs.prepare();

    for (auto &pOp: m_operations)
    {
        pOp->execute(m_pEnvMgr, &m_inputs);
    }

    return rc;
}
