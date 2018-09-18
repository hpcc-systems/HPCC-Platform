/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#ifndef _ENVGEN2_INCL
#define _ENVGEN2_INCL

//#include <vector>
#include "jliball.hpp"
#include "XMLTags.h"
#include "IConfigEnv.hpp"
#include "ConfigEnvFactory.hpp"

using namespace std;
using namespace ech;

#define ATTR_SEP  "^"
#define ATTR_V_SEP "|"


interface IPropertyTree;

//typedef vector<IPropertyTree*> IPropertyTreePtrArray;

class CEnvGen
{

public:
   CEnvGen();
   ~CEnvGen();

   bool parseArgs(int argc, char** argv);
   void createUpdateTask(const char* action, IPropertyTree* config, const char* param);
   void createUpdateNodeTask(const char* action, IPropertyTree * config, const char* param);
   void createUpdateBindingTask(const char* action, IPropertyTree * config, const char* param);
   void createUpdateServiceTask(const char* action, IPropertyTree * config, const char* param);
   void createUpdateTopologyTask(const char* action, IPropertyTree * config, const char* param);
   void addUpdateAttributesFromString(IPropertyTree *updateTree, const char *attrs);
   void addUpdateTaskFromFile(const char* inFile);
   bool convertOverrideTask(IPropertyTree * config, const char* input);
   bool process();


   void usage();
   void usage_update_input_format_1();
   void usage_update_input_format_2();
   void usage_update_input_format_3();
   void usage_update_input_format_3_json();

private:

   static map<string, string> m_envCategoryMap;
   static map<string, string> m_actionAbbrMap;

   IConfigEnv<IPropertyTree, StringBuffer>* m_iConfigEnv;
   //IPropertyTree *  params;
   Owned<IPropertyTree>  m_params;
   bool m_showInputOnly;
   int m_inputFormat;
   int m_displayFormat;

   StringBufferArray m_arrXPaths;
   StringBufferArray m_arrAttrib;
   StringBufferArray m_arrValues;

   StringBufferArray m_arrContentXPaths;
   StringBufferArray m_arrContentFormats;
   StringBufferArray m_arrContents;

   //StringArray m_arrAssignIPRanges;
   //StringArray m_arrBuildSetWithAssignedIPs;


};

#endif
