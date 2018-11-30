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

#include "Input.hpp"
#include "Inputs.hpp"
#include "TemplateException.hpp"


void Inputs::add(const std::shared_ptr<Input> pNewInput)
{
    for (auto &pInput: m_inputValues)
    {
        if (pInput->getName() == pNewInput->getName())
        {
            throw TemplateException("Input '" + pNewInput->getName() + "' is a duplicate.");
        }
    }

    m_inputValues.emplace_back(pNewInput);
}


std::shared_ptr<Input> Inputs::getInput(const std::string &name, bool throwIfNotFound) const
{
    std::shared_ptr<Input> pRetInput;
    for (auto &pInput: m_inputValues)
    {
        if (pInput->getName() == name)
        {
            pRetInput = pInput;
            break;
        }
    }

    if (!pRetInput && throwIfNotFound)
    {
        throw TemplateException("Unable to find input, name = '" + name + "'.");
    }
    return pRetInput;
}


void Inputs::prepare()
{
    for (auto &pInput: m_inputValues)
    {
        std::string preparedValue = pInput->getPreparedValue();
        if (!preparedValue.empty())
        {
            pInput->setValue(doValueSubstitution(preparedValue));
        }
    }
}


std::string Inputs::doValueSubstitution(const std::string &value) const
{
    //
    // A value has the form {{name}}[{{index}}] where name and index can be simple strings and the index is optional
    // Or {{name}}.size which will return the size of the variable name (number of entries)
    // Or name.size
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
        std::size_t bracketStartPos = result.find('[');
        std::size_t sizePos = result.find(".size");

        if (bracketStartPos != std::string::npos && sizePos != std::string::npos)
        {
            throw TemplateException("Both [] and .size may not appear in a variable input");
        }

        if (bracketStartPos != std::string::npos)
        {
            std::size_t bracketEndPos = findClosingDelimiter(result, bracketStartPos, "[", "]");  //  result.find(']');
            std::string indexStr = result.substr(bracketStartPos+1, bracketEndPos - bracketStartPos - 1);
            varName = result.substr(bracesStartPos + 2, bracketStartPos - bracesStartPos - 2);
            try {
                index = std::stoul(evaluate(doValueSubstitution(indexStr)));
            } catch (...) {
                throw TemplateException("Non-numeric count found for index value", false);
            }
        }

        if (sizePos != std::string::npos)
        {
            result = std::to_string(getInput(varName, true)->getNumValues());
        }
        else
        {
            std::string substitueValue = doValueSubstitution(getInput(varName, true)->getValue(index));
            std::string newResult = result.substr(0, bracesStartPos);
            newResult += substitueValue;
            newResult += result.substr(bracesEndPos + 2);
            result = newResult;
        }

        bracesStartPos = result.find("{{");
        done = bracesStartPos == std::string::npos;
    }

    //
    // This should NOT have a [] in it

    return evaluate(result);
}



std::size_t Inputs::findClosingDelimiter(const std::string &input, std::size_t startPos, const std::string &openDelim, const std::string &closeDelim) const
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


std::string Inputs::evaluate(const std::string &expr) const
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


void Inputs::clear()
{
    m_inputValues.clear();
}
