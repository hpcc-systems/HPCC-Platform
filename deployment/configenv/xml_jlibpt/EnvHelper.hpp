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
#ifndef _ENVHELPER_HPP_
#define _ENVHELPER_HPP_

#include "jiface.hpp"
#include "jliball.hpp"
#include "XMLTags.h"
//#include "IConfigEnv.hpp"
#include "ConfigEnvError.hpp"
#include "GenEnvRules.hpp"

namespace ech
{

//#define ATTR_SEP  "^"
//#define ATTR_V_SEP "|"

//interface IConfigEnv;

//interface IConfigComp;
interface IConfigComp : public IInterface
{
  virtual void create(IPropertyTree *params) = 0;
  virtual unsigned add(IPropertyTree *params) = 0;
  //virtual void addNode(IPropertyTree *node, const char* xpath, bool merge) = 0;
  virtual void modify(IPropertyTree *params) = 0;
  virtual void remove(IPropertyTree *params) = 0;
};

#define DEFAULT_ENV_XML CONFIG_DIR"/environment.xml"
#define DEFAULT_ENV_OPTIONS CONFIG_DIR"/environment.conf"
#define DEFAULT_GEN_ENV_RULES CONFIG_DIR"/genenvrules.conf"
#define DEFAULT_BUILDSET COMPONENTFILES_DIR"/configxml/buildset.xml"
#define ESP_CONFIG_PATH INSTALL_DIR "" CONFIG_DIR "/configmgr/esp.xml"

#define USE_WIZARD 1

enum CONFIG_TYPE { CONFIG_INPUT, CONFIG_ENV, CONFIG_ALL };

class EnvConfigOptions
{
public:
   EnvConfigOptions(const char* filename) { loadFile(filename); }
   void loadFile(const char* filename);
   const IProperties * getProperties() const { return m_options; }

private:
   Owned<IProperties> m_options;
};


class EnvHelper
{

public:
   EnvHelper(IPropertyTree * config);
   ~EnvHelper();
   const EnvConfigOptions& getEnvConfigOptions() const { return *m_envCfgOptions; }
   const GenEnvRules& getGenEnvRules() const { return *m_genEnvRules; }
   const IPropertyTree * getBuildSetTree() const { return m_buildSetTree;}
   IPropertyTree * getEnvTree() { return m_envTree; }
   IConfigComp* getEnvComp(const char * compName);
   IConfigComp* getEnvSWComp(const char * swCompName);
   const char* getConfig(const char* key, CONFIG_TYPE type=CONFIG_INPUT) const;
   EnvHelper * setEnvTree(StringBuffer &envXml);
   bool validateAndToInteger(const char *str,int &out, bool throwExcepFlag);
   const char* getXMLTagName(const char* name);


   // PTree helper
   IPropertyTree * clonePTree(const char* xpath);
   IPropertyTree * clonePTree(IPropertyTree *src);

   // ip address
   void processNodeAddress(IPropertyTree *params);
   int  processNodeAddress(const char * ipInfo, StringArray &ips, bool isFile=false);
   const StringArray& getNodeList() const { return m_ipArray; }
   bool getCompNodeList(const char * compName, StringArray *ipList, const char* cluster=NULL );
   const char * assignNode(const char * compName);
   //void  releaseNodeIp(const char * ip);

   //Node id
   //int categoryToId(const char* category);
   //const char*  idToCategory(int id);


private:

   void init(IPropertyTree * config);

   Owned<IPropertyTree> m_envTree;
   Owned<IPropertyTree> m_buildSetTree;
   EnvConfigOptions *m_envCfgOptions;
   GenEnvRules *m_genEnvRules;
   IPropertyTree * m_config;

   StringArray m_ipArray;
   int m_supportIpListPosition = 0;

   MapStringToMyClass<IConfigComp> m_compMap;
   MapStringTo<int> m_baseIds;
};

}
#endif
