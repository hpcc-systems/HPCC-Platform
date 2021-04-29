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

#include "Variable.hpp"
#include "Variables.hpp"
#include "TemplateException.hpp"


void Variables::add(const std::shared_ptr<Variable> pVariable)
{
    for (auto &pVar: m_variables)
    {
        if (pVar->getName() == pVariable->getName())
        {
            throw TemplateException("Variable '" + pVariable->getName() + "' is a duplicate.", true);
        }
    }

    m_variables.emplace_back(pVariable);
}


std::shared_ptr<Variable> Variables::getVariable(const std::string &name, bool throwIfNotFound) const
{
    std::shared_ptr<Variable> pRetVar;
    std::string varName = name;

    //
    // Accept both a regular string or a {{name}} string for the input name
    std::size_t bracesStartPos = name.find("{{");
    if (bracesStartPos != std::string::npos)
    {
        std::size_t bracesEndPos = findClosingDelimiter(name, bracesStartPos,"{{", "}}");
        varName = name.substr(bracesStartPos + 2, bracesEndPos - bracesStartPos - 2);
    }


    for (auto &pVar: m_variables)
    {
        if (pVar->getName() == varName)
        {
            pRetVar = pVar;
            break;
        }
    }

    if (!pRetVar && throwIfNotFound)
    {
        throw TemplateException("Unable to find variable, name = '" + name + "'.");
    }
    return pRetVar;
}


void Variables::prepare()
{
    for (auto &pVar: m_variables)
    {
        std::string preparedValue = pVar->getPreparedValue();
        if (!preparedValue.empty())
        {
            pVar->addValue(doValueSubstitution(preparedValue));
        }
    }
}


std::string Variables::doValueSubstitution(const std::string &value) const
{
    //
    // A value has the form {{name}}[{{index}}] where name and index can be simple strings and the index is optional
    // Or {{name}}.size which will return the size of the variable name (number of entries)
    std::string varName, result = value;
    std::size_t index;

    std::size_t bracesStartPos = result.find("{{");
    bool done = bracesStartPos == std::string::npos;

    while (!done)
    {
        index = m_curIndex;
        std::size_t bracesEndPos = findClosingDelimiter(result, bracesStartPos,"{{", "}}");
        varName = result.substr(bracesStartPos + 2, bracesEndPos - bracesStartPos - 2);

        //
        // If there is an index defined, evaluate it and update the index to be used for the final value
        std::size_t bracketStartPos = varName.find('[');
        std::size_t sizePos = varName.find(".size");

        if (bracketStartPos != std::string::npos && sizePos != std::string::npos)
        {
            throw TemplateException("Both [] and .size may not appear in a variable");
        }

        if (bracketStartPos != std::string::npos)
        {
            std::size_t bracketEndPos = findClosingDelimiter(varName, bracketStartPos, "[", "]");
            std::string indexStr = varName.substr(bracketStartPos+1, bracketEndPos - bracketStartPos - 1);
            varName = varName.substr(0, bracketStartPos);
            try {
                index = std::stoul(evaluate(doValueSubstitution(indexStr)));
            } catch (...) {
                throw TemplateException("Non-numeric count found for index value", false);
            }
            sizePos = varName.find(".size");
        }

        if (sizePos != std::string::npos)
        {
            std::string baseVarName = varName.substr(0, sizePos);
            result = std::to_string(getVariable(baseVarName, true)->getNumValues());
        }
        else
        {
            std::string substitueValue = doValueSubstitution(getVariable(varName, true)->getValue(index));
            std::string newResult = result.substr(0, bracesStartPos);
            newResult += substitueValue;
            newResult += result.substr(bracesEndPos + 2);
            result = newResult;
        }

        bracesStartPos = result.find("{{");
        done = bracesStartPos == std::string::npos;
    }

    return evaluate(result);
}



std::size_t Variables::findClosingDelimiter(const std::string &input, std::size_t startPos, const std::string &openDelim, const std::string &closeDelim) const
{
    std::size_t curPos = startPos + openDelim.length();
    std::size_t openPos, closePos;
    unsigned depth = 1;

    do
    {
        closePos = input.find(closeDelim, curPos);
        openPos = input.find(openDelim, curPos);

        if (closePos == std::string::npos)
        {
            throw TemplateException("Missing closing delimiter '" + closeDelim + "' string = '" + input + "'", false);
        }

        if (openPos != std::string::npos && closePos > openPos)
        {
            ++depth;
            curPos = openPos + openDelim.length();
        }
        else
        {
           if (--depth > 0)
           {
               if (openPos != std::string::npos)
               {
                   ++depth;
                   curPos = openPos + openDelim.length();
               }
               else
               {
                   curPos = closePos + closeDelim.length();
               }
           }
        }
    } while (depth > 0);

    return closePos;
}


std::string Variables::evaluate(const std::string &expr) const
{
    std::size_t opPos;
    std::string result = expr;

    opPos = expr.find_first_of("+-");

    if (opPos != std::string::npos)
    {
        long op1, op2, value;
        try {
            op1 = std::stol(expr.substr(0, opPos));
        } catch (...) {
            throw TemplateException("Non-numeric operand 1 found in expression '" + expr + "'", false);
        }

        try {
            op2 = std::stol(expr.substr(opPos+1));
        } catch (...) {
            throw TemplateException("Non-numeric operand 2 found in expression '" + expr + "'", false);
        }
        if (expr[opPos] == '-')
            value = op1 - op2;
        else
            value = op1 + op2;

        if (value < 0)
        {
            throw TemplateException("Result of expression '" + expr + "' is negative, only >0 results allowed", false);
        }

        result = std::to_string(value);
    }
    return result;
}


void Variables::clear()
{
    m_variables.clear();
}
