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

#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include "platform.h"
#include <exception>
#include <sys/types.h>
#include <sys/stat.h>
#include "EnvironmentMgr.hpp"
#include "Exceptions.hpp"
#include "mod_template_support/EnvModTemplate.hpp"
#include "mod_template_support/TemplateExecutionException.hpp"
#include "jutil.hpp"

//
// Inputs file format (json)
//
// {
//   "input-name" : [ <array of values> ],
//   ...
// }

bool processOptions(int argc, char *vargv[]);
bool validate();
std::string getNextArg(int argc, char *argv[], int idx);
bool dirExists(const std::string &dirName);
bool fileExists(const std::string &fileName);
std::vector<std::string> splitString(const std::string &input, const std::string &delim);

// Default configuration directories
EnvironmentType envType = XML;
std::string masterSchemaFile = "environment.xsd";
std::string configSchemaDir = "";
std::string modTemplateFile;
std::string configSchemaPluginsDir = "";
std::string envFile, envOutputFile;
struct InputDef {
    std::string inputName;
    std::string inputValue;
};

std::vector<InputDef> userInputs;
bool listInputs = false;

EnvironmentMgr *pEnvMgr = nullptr;
EnvModTemplate *pTemplate = nullptr;

class CliException : public std::exception
{
    public:

        explicit CliException(std::string reason) :  m_reason(std::move(reason)) { }
        virtual const char *what() const noexcept override
        {
            return m_reason.c_str();
        }

    private:

        std::string m_reason;
};


void usage()
{
    //
    // usage below documents options
    std::cout << std::endl;
    std::cout << "envmod <options> envfile" << std::endl;
    std::cout << std::endl;
    std::cout << "  Configuration Options:" << std::endl;
    std::cout << "    -d --schema-dir <path>            : path to schema files. default (" << configSchemaDir << ")" << std::endl;
    std::cout << "    -p --schema-plugins-dir <path>    : path to plugin files, default (" << configSchemaPluginsDir << ")" << std::endl;
    std::cout << "    -m --master-config <filename>     : name of master schema file, default (" << masterSchemaFile << ")" << std::endl;
    std::cout << std::endl;
    std::cout << "  Execution Options:" << std::endl;
    std::cout << "    -t --template <filepath>          : filepath to modification template - required" << std::endl;
    std::cout << "    -e --env <fullpath>               : Optional filepath to environment file to modify" << std::endl;
    std::cout << "                                          Omit to validate the modification template" << std::endl;
    std::cout << "       --inputs                       : List the template inputs. If this option is specified, the template" << std::endl;
    std::cout << "                                          inputs are listed and the command exits." << std::endl;
    std::cout << "       --input-file <fullpath>        : Optional full path to a file with defined inputs for the template" << std::endl;
    std::cout << "                                          These would be combined with any command line inputs" << std::endl;
    std::cout << "    -i --input <var=value>            : Assign the indicated value to the variable input." << std::endl;
    std::cout << "                                          The input must be defined in the template" << std::endl;
    std::cout << "                                          value may be a comma separated list (no blanks) for multi-value inputs" << std::endl;
    std::cout << "                                          Inputs given on the command line override any in an input file (--input-file)" << std::endl;
    std::cout << "    -o --output [fullpath]            : Optional. If present, results of applying template are saved." << std::endl;
    std::cout << "                                          If fullpath is specified, results written to this file, " << std::endl;
    std::cout << "                                          If not specified, input env file is overwritten" << std::endl;
    std::cout << "                                          Omit the -o option to test the modification template" << std::endl;
    std::cout << std::endl;
}


int main(int argc, char *argv[])
{
    std::string modTemplateSchemaFile = std::string(hpccBuildInfo.componentDir) + PATHSEPSTR + "configschema" + PATHSEPSTR + "templates" + PATHSEPSTR + "schema" + PATHSEPSTR + "ModTemplateSchema.json";
    
    std::string processPath(queryCurrentProcessPath());

    configSchemaDir = std::string(hpccBuildInfo.componentDir) + PATHSEPSTR + "configschema" + PATHSEPSTR + "xsd" + PATHSEPSTR;

    if (argc == 1)
    {
        usage();
        return 0;
    }

    //
    // Read options and validate
    if (!processOptions(argc, argv))
    {
        return 1;   // get out now
    }

    if (!validate())
    {
        usage();
        return 1;
    }

    //
    // Create an environment manager reference and load the schema
    try
    {
        pEnvMgr = getEnvironmentMgrInstance(envType);
        std::cout << "Loading schema defined by " << masterSchemaFile << "...";

        // note that these are hardcoded for HPCC at this time, but could be made into options
        std::map<std::string, std::string> cfgParms;
        cfgParms["buildset"] = "buildset.xml";  // Not used right now, and probably never will be
        cfgParms["support_libs"] = "libcfgsupport_addrequiredinstances";
        std::string pluginsPath = configSchemaPluginsDir;

        if (!pEnvMgr->loadSchema(configSchemaDir, masterSchemaFile, cfgParms))
        {
            throw CliException(pEnvMgr->getLastSchemaMessage());
        }
    }
    catch (const ParseException &pe)
    {
        std::cout << "There was a problem creating the environment manager instance: " << pe.what() << std::endl;
    }
    catch(const CliException &e)
    {
        std::cout << "There was a problem loading the schema: " << e.what() << std::endl;
    }


    //
    // Create the modification template
    try
    {
        pTemplate = new EnvModTemplate(pEnvMgr, modTemplateSchemaFile);
        pTemplate->loadTemplateFromFile(modTemplateFile);
    }
    catch (const TemplateException &te)
    {
        std::cout << "There was a problem loading the modification template: " << te.what() << std::endl;
        return 1;
    }

    //
    // If list inputs was given, list them and get out
    if (listInputs)
    {
        std::cout << "Template inputs:" << std::endl;
        auto templateInputs = pTemplate->getVariables();
        for (auto &pInput : templateInputs)
        {
            std::cout << pInput->getName() << " - " << pInput->getDescription() << std::endl;
        }
        return 0;  // get out
    }

    //
    // Process the input file if given here

    //
    // If the user provided any inputs, assign them
    if (!userInputs.empty())
    {
        for (auto &userInput: userInputs)
        {
            try
            {
                auto pInput = pTemplate->getVariable(userInput.inputName);
                auto values = splitString(userInput.inputValue, ",");
                for (auto &value: values)
                {
                    pInput->addValue(value);
                }
            }
            catch (const TemplateException &te)
            {
                std::cout << "There was a problem: " << te.what() << std::endl;
                return 0;  // get out
            }
        }
    }

    //
    // If there is an environment, load it and apply
    if (!envFile.empty())
    {
        if (!pEnvMgr->loadEnvironment(envFile))
        {
            std::cout << "There was a problem loading the environment: " << std::endl << pEnvMgr->getLastEnvironmentMessage() << std::endl;
            return 1;
        }

        try
        {
            pTemplate->execute();
        }
        catch (const TemplateExecutionException &te)
        {
            std::cout << te.what() << std::endl;
            return 1;
        }

        //
        // Write results
        if (!envOutputFile.empty())
        {
            pEnvMgr->saveEnvironment(envOutputFile);
            std::cout << "Results written to " << envOutputFile << std::endl;
        }
        else
        {
            std::cout << "Resuls not saved." << std::endl;
        }
    }
    else
    {
        std::cout << "No problems found in the modification template" << std::endl;
    }

    std::cout << "Done" << std::endl;
    return 0;
}


bool processOptions(int argc, char *argv[])
{
    bool rc = true;
    int idx = 1;
    std::string optName, optVal;
    bool checkDir = false;

    try
    {
        while (idx < argc)
        {
            optName = getNextArg(argc, argv, idx++);

            if (optName == "-d" || optName == "--schema-dir")
            {
                configSchemaDir = getNextArg(argc, argv, idx++) += PATHSEPSTR;
            }

            else if (optName == "-p" || optName == "--schema-plugins-dir")
            {
                configSchemaPluginsDir = getNextArg(argc, argv, idx++) += PATHSEPSTR;
            }

            else if (optName == "-m" || optName == "--master-config")
            {
                masterSchemaFile = getNextArg(argc, argv, idx++);
            }

            else if (optName == "-t" || optName == "--template")
            {
                modTemplateFile = getNextArg(argc, argv, idx++);
            }
            else if (optName == "-e" || optName == "--env")
            {
                envFile = getNextArg(argc, argv, idx++);
            }
            else if (optName == "-o" || optName == "--output")
            {
                // this is the default if no filename given
                envOutputFile = envFile;
                if (idx < argc)
                {
                    envOutputFile = getNextArg(argc, argv, idx++);
                }
            }
            else if (optName == "-i" || optName == "--input")
            {
                std::string assign = getNextArg(argc, argv, idx++);
                std::size_t equalPos = assign.find_first_of('=');
                if (equalPos != std::string::npos)
                {
                    InputDef input;
                    input.inputName = assign.substr(0, equalPos);
                    input.inputValue = assign.substr(equalPos+1);
                    userInputs.emplace_back(input);
                }
                else
                {
                    throw CliException("Invalid input variable assignement: " + assign);
                }
            }
            else if (optName == "--inputs")
            {
                listInputs = true;
            }

        }
    }
    catch(const CliException &e)
    {
        std::cout << "There was an issue processing option: " << e.what() << std::endl;
        rc = false;
    }
    return rc;
}


bool validate()
{

    if (!dirExists(configSchemaDir))
    {
        std::cout << "Schema directory " << configSchemaDir << " does not exist" << std::endl;
        return false;
    }

    if (!dirExists(configSchemaPluginsDir))
    {
        std::cout << "Schema plugins directory " << configSchemaPluginsDir << " does not exist" << std::endl;
        return false;
    }

    if (!fileExists(configSchemaDir + masterSchemaFile))
    {
        std::cout << "The master config file " << masterSchemaFile << " does not exist" << std::endl;
        return false;
    }

    if (!fileExists(modTemplateFile))
    {
        std::cout << "The modification template " << modTemplateFile << " does not exist" << std::endl;
        return false;
    }

    if (!envFile.empty())
    {
        if (!fileExists(envFile))
        {
            std::cout << "The environment file " << envFile << " does not exist" << std::endl;
            return false;
        }
    }

    return true;
}


std::string getNextArg(int argc, char *argv[], int idx)
{
    if (idx < argc)
    {
        return std::string(argv[idx]);
    }
    throw CliException("Arguments exhausted when more expected");
}


bool dirExists(const std::string &dirName)
{
    bool rc = true;
    struct stat info;
    if (stat(dirName.c_str(), &info) != 0)
    {
        rc = false;
    }
    rc = ((info.st_mode&S_IFMT)==S_IFDIR);
    return rc;
}


bool fileExists(const std::string &fileName)
{
    struct stat info;
    return stat(fileName.c_str(), &info) == 0;
}


std::vector<std::string> splitString(const std::string &input, const std::string &delim)
{
    size_t  start = 0, end = 0, delimLen = delim.length();
    std::vector<std::string> list;

    while (end != std::string::npos)
    {
        end = input.find(delim, start);
        std::string item = input.substr(start, (end == std::string::npos) ? std::string::npos : end - start);
        if (!item.empty())
            list.push_back(item);
        start = ((end > (std::string::npos - delimLen)) ? std::string::npos : end + delimLen);
    }
    return list;
}
