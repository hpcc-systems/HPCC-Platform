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
#include "Status.hpp"
#include "jutil.hpp"

//
// Inputs file format (json)
//
// {
//   "input-name" : [ <array of values> ],
//   ...
// }

bool processOptions(int argc, char *argv[]);
bool validate();
std::string getNextArg(int argc, char *argv[], int idx);
bool dirExists(const std::string &dirName);
bool fileExists(const std::string &fileName);
std::vector<std::string> splitString(const std::string &input, const std::string &delim);
void outputStatus(Status &status, enum statusMsg::msgLevel);

// Default configuration directories
EnvironmentType envType = XML;
std::string masterSchemaFile = "environment.xsd";
std::string configSchemaRelativeDir = ".." PATHSEPSTR "componentfiles"  PATHSEPSTR "configschema" PATHSEPSTR "xsd" PATHSEPSTR;
std::string configSchemaDir = "";
std::string configSchemaPluginsDir = "";
std::string envFile;
enum statusMsg::msgLevel outputLevel = statusMsg::warning;
bool verbose = false;
bool validateHiddenNodes = false;


EnvironmentMgr *pEnvMgr = nullptr;

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
    std::cout << "envchk <options> envfile" << std::endl;
    std::cout << std::endl;
    std::cout << "    -d --schema-dir <path>            : path to schema files. default (" << configSchemaDir << ")" << std::endl;
    std::cout << "    -p --schema-plugins-dir <path>    : path to plugin files, default (" << configSchemaPluginsDir << ")" << std::endl;
    std::cout << "    -m --master-config <filename>     : name of master schema file, default (" << masterSchemaFile << ")" << std::endl;
    std::cout << "    -l --level <level>                : output level (and above), e=error, w=warning, i=informational" << std::endl;
    std::cout << "    -v --verbose                      : verbose output" << std::endl;
    std::cout << std::endl;
}


int main(int argc, char *argv[])
{
    int rc = 0;

    //
    // Build the default directory for the schema files
    std::string processPath(queryCurrentProcessPath());
    configSchemaDir = processPath.substr(0, processPath.find_last_of(PATHSEPSTR)) + PATHSEPSTR + configSchemaRelativeDir;

    //
    // Print usage?
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
        if (verbose)
            std::cout << "Loading schema defined by " << masterSchemaFile << std::endl;

        // note that these are hardcoded for HPCC at this time, but could be made into options
        std::map<std::string, std::string> cfgParms;
        cfgParms["buildset"] = "buildset.xml";  // Not used right now, and probably never will be
        std::string pluginsPath = configSchemaPluginsDir;

        if (!pEnvMgr->loadSchema(configSchemaDir, masterSchemaFile, cfgParms))
        {
            throw CliException(pEnvMgr->getLastSchemaMessage());
        }

    }
    catch (const ParseException &pe)
    {
        std::cerr << "There was a problem creating the environment manager instance: " << pe.what() << std::endl;
    }
    catch(const CliException &e)
    {
        std::cerr << "There was a problem loading the schema: " << e.what() << std::endl;
    }

    //
    // If there is an environment, load it and apply
    if (!envFile.empty())
    {
        if (pEnvMgr->loadEnvironment(envFile))
        {
            Status status;
            pEnvMgr->validate(status, validateHiddenNodes);
            outputStatus(status, outputLevel);
        }
        else
        {
            std::cerr << "There was a problem loading the environment: " << std::endl << pEnvMgr->getLastEnvironmentMessage() << std::endl;
            rc = 1;
        }
    }

    return rc;
}


void outputStatus(Status &status, enum statusMsg::msgLevel lvl)
{
    for (int curLevel=lvl; curLevel <= statusMsg::fatal; ++curLevel)
    {
        std::vector<statusMsg> msgs = status.getMessages(static_cast<enum statusMsg::msgLevel>(curLevel), false);
        for (auto &msg: msgs)
        {
            std::string path;
            if (!msg.nodeId.empty())
            {
                auto pNode = pEnvMgr->findEnvironmentNodeById(msg.nodeId);
                if (pNode)
                {
                    pNode->getPath(path);
                }
            }
            std::cerr << status.getStatusTypeString(msg.msgLevel) << " : Path=" << path;
            if (!msg.attribute.empty())
                std::cerr << "[" << msg.attribute << "]";
            std::cerr << " Message=" << msg.msg << std::endl;
        }
    }
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

            else if (optName == "-l" || optName == "--level")
            {
                std::string lvl = getNextArg(argc, argv, idx++);
                {
                    if (lvl == "e")
                    {
                        outputLevel = statusMsg::error;
                    }
                    else if (lvl == "w")
                    {
                        outputLevel = statusMsg::warning;
                    }
                    else if (lvl == "i")
                    {
                        outputLevel = statusMsg::info;
                    }
                    else
                    {
                        throw CliException("Output level is not valid");
                    }
                }
            }

            else if (optName == "-v" || optName == "--verbose")
            {
                verbose = true;
            }

            else if (idx == argc)
            {
                envFile = optName;
            }

            else
            {
                throw CliException("Parameters are incorrect");
            }
        }
    }
    catch(const CliException &e)
    {
        std::cerr << "There was an issue processing options: " << e.what() << std::endl;
        rc = false;
    }
    return rc;
}


bool validate()
{

    if (!dirExists(configSchemaDir))
    {
        std::cerr << "Schema directory " << configSchemaDir << " does not exist" << std::endl;
        return false;
    }

    if (!configSchemaPluginsDir.empty() && !dirExists(configSchemaPluginsDir))
    {
        std::cerr << "Schema plugins directory " << configSchemaPluginsDir << " does not exist" << std::endl;
        return false;
    }

    if (!fileExists(configSchemaDir + masterSchemaFile))
    {
        std::cerr << "The master config file " << masterSchemaFile << " does not exist" << std::endl;
        return false;
    }

    if (!envFile.empty())
    {
        if (!fileExists(envFile))
        {
            std::cerr << "The environment file " << envFile << " does not exist" << std::endl;
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

        if (end != std::string::npos)
        {
            start = end + delimLen;
            if (start >= input.length())
                end = std::string::npos;
        }
    }
    return list;
}
