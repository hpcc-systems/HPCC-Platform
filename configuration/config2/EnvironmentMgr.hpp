/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems

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
#include "ConfigItem.hpp"
#include "ConfigParser.hpp"
#include "EnvironmentNode.hpp"
#include "Status.hpp"

class EnvironmentMgr;

EnvironmentMgr *getEnvironmentMgrInstance(const std::string &envType, const std::string &configPath);


class EnvironmentMgr
{
	public:

		struct valueDef {
			std::string name;
			std::string value;
		};

        EnvironmentMgr(const std::string &configPath);
		virtual ~EnvironmentMgr() { }

		// add a load from stream?
        bool loadConfig(const std::vector<std::string> &cfgParms);  // parms are dependent on the environment type
		bool loadEnvironment(const std::string &file);  // return some error code,or a get last error type of call?

        std::shared_ptr<EnvironmentNode> getEnvironmentNode(const std::string &nodeId);
		//void setAttributeValues(const std::string &path, const std::vector<valueDef> &values, const std::string &nodeValue, bool force=false);
		
		// save to stream ?
		void saveEnvironment(const std::string &file, Status &status);
        void validate(Status &status) const;


	protected:

		std::string getUniqueKey();
		
		void addPath(const std::shared_ptr<EnvironmentNode> pNode);
        virtual bool createParser(const std::vector<std::string> &cfgParms) = 0;
		virtual bool load(std::istream &in) = 0;
		virtual void save(std::ostream &out) = 0;


	protected:

        std::string m_configPath;
		std::shared_ptr<ConfigItem> m_pConfig;
        std::shared_ptr<ConfigParser> m_pConfigParser;
		std::shared_ptr<EnvironmentNode> m_pRootNode;
		std::map<std::string, std::shared_ptr<EnvironmentNode>> m_nodeIds;


	private:
		
		int m_key;
};

#endif