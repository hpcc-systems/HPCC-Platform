/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems�.

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

#include "EnvironmentMgr.hpp"
#include "ConfigExceptions.hpp"
#include "XMLEnvironmentMgr.hpp"



EnvironmentMgr *getEnvironmentMgrInstance(const std::string &envType, const std::string &configPath)
{
    EnvironmentMgr *pEnvMgr = NULL;
    if (envType == "XML")
    {
        //std::shared_ptr<ConfigParser> pCfgParser = std::make_shared<XSDConfigParser>(configPath, m_pConfig);
        pEnvMgr = new XMLEnvironmentMgr(configPath);
    }
    return pEnvMgr;
}


EnvironmentMgr::EnvironmentMgr(const std::string &configPath) :
    m_key(0), 
    m_configPath(configPath)
{
    m_pConfig = std::make_shared<ConfigItem>("root");  // make the root
}


bool EnvironmentMgr::loadConfig(const std::vector<std::string> &cfgParms)
{
    createParser(cfgParms);
    m_pConfigParser->parseEnvironmentConfig(cfgParms);
    m_pConfig->postProcessConfig();
    return true;
}


bool EnvironmentMgr::loadEnvironment(const std::string &filename)
{
	std::ifstream in;
    std::string fpath = m_configPath + filename;
	
	in.open(fpath);
	if (in.is_open())
	{
		load(in);
	}
	return true;
}


void EnvironmentMgr::saveEnvironment(const std::string &filename)
{
	std::ofstream out;

	out.open(filename);
	if (out.is_open())
	{
		save(out);
	}
}


void EnvironmentMgr::addPath(const std::shared_ptr<EnvironmentNode> pNode)
{
	auto retVal = m_paths.insert({pNode->getPath(), pNode });
	if (!retVal.second)
	{
		throw (new ParseException("Attempted to insert duplicate path name " + pNode->getPath() + " for node "));
	}
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::getElement(const std::string &path)
{
	auto pathIt = m_paths.find(path);
	return pathIt->second;
}


// todo: make a standard return that has a status string and array of messages
bool EnvironmentMgr::setValuesForPath(const std::string &path, const std::vector<valueDef> &values, const std::string &nodeValue, bool force)
{
	std::shared_ptr<EnvironmentNode> pEnvNode = getElement(path);
	if (pEnvNode)
	{
		for (auto it = values.begin(); it != values.end(); ++it)
			pEnvNode->setAttributeValue((*it).name, (*it).value, force);
	}
	return true;
}


std::string EnvironmentMgr::getUniqueKey()
{
	return std::to_string(m_key++);
}


bool EnvironmentMgr::validate()
{
	if (m_pRootNode)
	{

	}
	return true;
}