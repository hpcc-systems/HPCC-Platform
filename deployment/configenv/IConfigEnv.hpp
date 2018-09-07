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

#ifndef _ICONFIGENV_HPP_
#define _ICONFIGENV_HPP_

//#include "ConfigEnvError.hpp"


#ifdef CONFIGENV_EXPORTS
  #define configenv_decl DECL_EXPORT
#else
  #define configenv_decl DECL_IMPORT
#endif

//Output format
//XML_SortTags | XML_Format

#ifndef ATTR_SEP
#define ATTR_SEP  "^"
#endif

#ifdef ATTR_V_SEP
#define ATTR_V_SEP "|"
#endif


interface IPropertyTree;
interface IEnvSettings;


//interface configenv_decl IConfigEnv: extends IConfigComp

template <class PTTYPE, class SB>
interface configenv_decl IConfigEnv
{
  virtual void create(PTTYPE *params) = 0;
  // return the id of the added node
  virtual unsigned add(PTTYPE *params) = 0;
  virtual void modify(PTTYPE *params) = 0;
  virtual void remove(PTTYPE *params) = 0;
  //run a group a updat tasks: <Config><Task/><Task/>...</Config>
  virtual void runUpdateTasks(PTTYPE *params) = 0;

  virtual const char* queryAttribute(const char *xpath) = 0;
  virtual void setAttribute(const char *xpath, const char* attrName, const char* attrValue) = 0;

  // xpath NULL will get whole environment
  virtual void getContent(const char *xpath, SB& out, int format) = 0;
  // Default format is XML. This will be direct action. No checking and relationship will be applied.
  virtual void addContent(const char *xpath, StringBuffer& in, int type) = 0;

  virtual bool isEnvironmentValid(SB& env) = 0;

  virtual PTTYPE * getNode(const char *xpath) = 0;
  virtual PTTYPE * getNode(unsigned id) = 0;
  virtual unsigned getNodeId(const char *xpath) = 0;

  // validate value from env tree or schema, xpath and schema
  virtual bool isAttributeValid(const char* xpath, const char* schema, const char* key, const char* value, bool src) = 0;
};

#endif
