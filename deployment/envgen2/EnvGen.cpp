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

#include <map>
#include <string>
#include "XMLTags.h"
#include "jliball.hpp"
#include "EnvGen.hpp"


map<string, string> CEnvGen::m_envCategoryMap =
{
  {""  , "Software"},
  {"sw", "Software"},
  {"pg", "Programs"},
  {"es", "EnvSettings"},
  {"hd", "Hardware"}
};

map<string, string> CEnvGen::m_actionAbbrMap =
{
  {"add"  , "add"},
  {"modify", "mod"},
  {"remove", "rmv"}
};

CEnvGen::CEnvGen()
{
   m_showInputOnly = false;
   m_inputFormat = XML_Format;
   m_displayFormat = XML_Format;
   m_iConfigEnv = NULL;
}

CEnvGen::~CEnvGen()
{
   if (m_iConfigEnv) ConfigEnvFactory::destroy(m_iConfigEnv);
}

bool CEnvGen::parseArgs(int argc, char** argv)
{
   int i = 1;

   //m_params = createPTree("Env");
   m_params.setown(createPTree("Env"));
   IPropertyTree * config = createPTree("Config");
   m_params->addPropTree("Config", config);

   while (i < argc)
   {
     if (stricmp(argv[i], "-help") == 0 || stricmp(argv[i], "-?") == 0)
     {
        usage();
        return false;
     }
     else if (stricmp(argv[i], "-mode") == 0)
     {
        i++;
        config->setProp("@mode", argv[i++]);
     }
     else if ((stricmp(argv[i], "-env") == 0) || (stricmp(argv[i], "-env-out") == 0))
     {
        i++;
        config->addProp("@env-out", argv[i++]);
     }
     else if (stricmp(argv[i], "-env-in") == 0)
     {
        i++;
        config->addProp("@env-in", argv[i++]);
     }
     else if (stricmp(argv[i], "-ip") == 0)
     {
        i++;
        config->addProp("@ip-list", argv[i++]);
     }
     else if (stricmp(argv[i], "-ipfile") == 0)
     {
        i++;
        config->addProp("@ip-file", argv[i++]);
     }
     else if (stricmp(argv[i], "-supportnodes") == 0)
     {
        i++;
        config->addProp("@support-nodes", argv[i++]);
     }
     else if (stricmp(argv[i], "-espnodes") == 0)
     {
        i++;
        config->addProp("@esp-nodes", argv[i++]);
     }
     else if (stricmp(argv[i], "-roxienodes") == 0)
     {
        i++;
        config->addProp("@roxie-nodes", argv[i++]);
     }
     else if (stricmp(argv[i], "-thornodes") == 0)
     {
        i++;
        config->addProp("@thor-nodes", argv[i++]);
     }
     else if (stricmp(argv[i], "-slavespernode") == 0)
     {
        i++;
        config->addProp("@slaves-per-node", argv[i++]);
     }
     else if (stricmp(argv[i], "-thorchannelsperslave") == 0)
     {
        i++;
        config->addProp("@thor-channels-per-slave", argv[i++]);
     }
     else if (stricmp(argv[i], "-roxiechannelsperslave") == 0)
     {
        i++;
        config->addProp("@roxie-channels-per-slave", argv[i++]);
     }
     else if (stricmp(argv[i], "-roxieondemand") == 0)
     {
        i++;
        if (!strcmp(argv[i++], "0"))
           config->addProp("@roxie-on-demand", "false");
     }
     else if (stricmp(argv[i], "-o") == 0)
     {
        i++;
        StringArray sbarr;
        sbarr.appendList(argv[i++], "=");
        if (sbarr.length() != 2)
           continue;
        if (strstr(sbarr.item(1), "[NAME]") && (strstr(sbarr.item(1), "[INST]") || strstr(sbarr.item(1), "[COMPONENT]")))
        {
           StringBuffer sb;
           sb.clear().appendf("sw:dirs:category@%s=%s",sbarr.item(0), sbarr.item(1));
           createUpdateTask("modify", config, sb.str());
        }
        else
        {
           fprintf(stderr, "Error: Directory Override must contain [NAME] and either [INST] or [COMPONENT]\n");
           return false;
        }

     }
     else if (stricmp(argv[i], "-override") == 0)
     {
        i++;
        if (!convertOverrideTask(config, argv[i++]))
           return false;
     }
     else if (stricmp(argv[i], "-set-xpath-attrib-value")== 0)
     {
        i++;
        m_arrXPaths.append(*new StringBufferItem (argv[i++]));
        m_arrAttrib.append(*new StringBufferItem (argv[i++]));
        m_arrValues.append(*new StringBufferItem (argv[i++]));
     }

     // new parameters
     else if (stricmp(argv[i], "-env-options") == 0)
     {
        i++;
        config->addProp("@options", argv[i++]);
     }
     else if (stricmp(argv[i], "-env-rules") == 0)
     {
        i++;
        config->addProp("@rules", argv[i++]);
     }
     else if (stricmp(argv[i], "-buildset") == 0)
     {
        i++;
        config->addProp("@buildset", argv[i++]);
     }
     else if (stricmp(argv[i], "-add") == 0)
     {
       i++;
       createUpdateTask("add", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-mod") == 0)
     {
       i++;
       createUpdateTask("modify", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-rmv") == 0)
     {
       i++;
       createUpdateTask("remove", config, argv[i++]);
     }

     else if (stricmp(argv[i], "-add-node") == 0)
     {
       i++;
       createUpdateNodeTask("add", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-mod-node") == 0)
     {
       i++;
       createUpdateNodeTask("modify", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-rmv-node") == 0)
     {
       i++;
       createUpdateNodeTask("remove", config, argv[i++]);
     }

     else if (stricmp(argv[i], "-add-binding") == 0)
     {
       i++;
       createUpdateBindingTask("add", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-mod-binding") == 0)
     {
       i++;
       createUpdateBindingTask("modify", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-rmv-binding") == 0)
     {
       i++;
       createUpdateBindingTask("remove", config, argv[i++]);
     }

     else if (stricmp(argv[i], "-add-service") == 0)
     {
       i++;
       createUpdateServiceTask("add", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-mod-service") == 0)
     {
       i++;
       createUpdateServiceTask("modify", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-rmv-service") == 0)
     {
       i++;
       createUpdateServiceTask("remove", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-add-topology") == 0)
     {
       i++;
       createUpdateTopologyTask("add", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-mod-topology") == 0)
     {
       i++;
       createUpdateTopologyTask("modify", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-rmv-topology") == 0)
     {
       i++;
       createUpdateTopologyTask("remove", config, argv[i++]);
     }
     else if (stricmp(argv[i], "-in-file") == 0)
     {
       i++;
       addUpdateTaskFromFile(argv[i++]);
     }
     else if (stricmp(argv[i], "-add-content")== 0)
     {
        i++;
        m_arrContentXPaths.append(*new StringBufferItem (argv[i++]));
        const char* fileName = argv[i++];
        StringBufferItem * sbi = new StringBufferItem() ;
        sbi->loadFile(fileName);
        m_arrContents.append(*sbi);
        if ((String(fileName).toLowerCase())->endsWith(".json"))
           m_arrContentFormats.append(*new StringBufferItem("json"));
        else
           m_arrContentFormats.append(*new StringBufferItem("xml"));

     }
     else if (stricmp(argv[i], "-show-input-only") == 0)
     {
        i++;
        m_showInputOnly = true;
        m_displayFormat = XML_Format;
     }
     else if (stricmp(argv[i], "-show-input-json-only") == 0)
     {
        i++;
        m_showInputOnly = true;
        m_displayFormat = JSON_Format;
     }
     else if (stricmp(argv[i], "-help-update-1") == 0)
     {
        usage_update_input_format_1();
        return false;
     }
     else if (stricmp(argv[i], "-help-update-2") == 0)
     {
		usage_update_input_format_2();
        return false;
     }
     else if (stricmp(argv[i], "-help-update-3") == 0)
     {
        usage_update_input_format_3();
        return false;
     }
     else if (stricmp(argv[i], "-help-update-3-json") == 0)
     {
        usage_update_input_format_3_json();
        return false;
     }
     else if (stricmp(argv[i], "-cloud") == 0)
     {
        i++;
        cloudConfiguration(config, argv[i++]);
     }
     else
     {
        fprintf(stderr, "\nUnknown option %s\n", argv[i]);
        usage();
        return false;
     }
   }

   // Check input parameters
   if (!config->queryProp("@env-out"))
   {
      fprintf(stderr, "\nMissing -env-out\n");
      usage();
      return false;
   }

   if (!config->queryProp("@mode"))
   {
      if (config->queryProp("@env-in"))
         config->addProp("@mode", "update");
      else
         config->addProp("@mode", "create");
   }

   if (!stricmp(config->queryProp("@mode"), "update") &&
       !config->queryProp("@env-in"))
   {
       fprintf(stderr, "\nMissing input enviroment.xml (-env-in) in update mode\n");
       return false;
   }

   if (!stricmp(config->queryProp("@mode"), "create") &&
       !config->hasProp("@ip-list")  && !config->hasProp("@ip-file"))
   {
      config->setProp("@support-nodes", "0");
      config->setProp("@roxie-nodes", "0");
      config->setProp("@thor-nodes", "0");
      config->setProp("@esp-nodes", "0");
   }


   m_iConfigEnv =  ConfigEnvFactory::getIConfigEnv(config);

   return true;
}

void CEnvGen::createUpdateTask(const char* action, IPropertyTree * config, const char* param)
{
   if (!param || !(*param)) return;

   if (m_showInputOnly)
   {
      printf("Input as one line format: -%s %s\n", (m_actionAbbrMap.at(action)).c_str(), param);
   }

   StringArray items;
   items.appendList(param, ":");
   if (items.ordinality() < 2)
   {
      throw MakeStringException(CfgEnvErrorCode::InvalidParams,
         "Incorrect input format. At least two item expected: category:component.\n See usage for deail.");
   }

   IPropertyTree * updateTree =  createPTree("Task");
   config->addPropTree("Task", updateTree);

   updateTree->addProp("@operation", action);
   updateTree->addProp("@category", (m_envCategoryMap.at(items.item(0))).c_str());

   //have key and attributes
   StringArray compAndAttrs;
   compAndAttrs.appendList(items.item(1), "@");

   StringArray compAndTarget;
   compAndTarget.appendList(compAndAttrs[0], "%");
   if (!stricmp(action, "remove") && compAndTarget.ordinality() > 1 )
   {
      if (*(compAndTarget.item(1))) updateTree->addProp("@target", compAndTarget.item(1));
   }

   StringArray compAndKey;
   compAndKey.appendList(compAndTarget.item(0), "#");

   updateTree->addProp("@component", compAndKey.item(0));

   if (compAndKey.ordinality() > 1)
   {
      StringArray keyAndClone;
      keyAndClone.appendList(compAndKey.item(1), ATTR_V_SEP);
      updateTree->addProp("@key", keyAndClone.item(0));
      if (keyAndClone.ordinality() > 1)
         updateTree->addProp("@clone", keyAndClone.item(1));
   }

   if (compAndAttrs.ordinality() > 1)
   {
      addUpdateAttributesFromString(updateTree, compAndAttrs.item(1));
      return;
   }

   if (items.ordinality() == 2)
      return;

   int index = 2;

   // selector
   StringArray selectorAndAttrs;
   selectorAndAttrs.appendList(items.item(index), "@");

   StringArray selectorParts;
   selectorParts.appendList(selectorAndAttrs.item(0), "/");

   StringBuffer sbSelector;
   for ( unsigned i = 0; i < selectorParts.ordinality()-1 ; i++)
   {
       if (!sbSelector.isEmpty())
          sbSelector.append("/");
       sbSelector.append(selectorParts.item(i));
   }

   StringArray selectorAndKey;
   selectorAndKey.appendList(selectorParts.item(selectorParts.ordinality()-1), "#");
   if (!sbSelector.isEmpty())
      sbSelector.append("/");
   sbSelector.append(selectorAndKey.item(0));
   sbSelector.replace('#', '@');

   updateTree->addProp("@selector", sbSelector.str());
   if (selectorAndKey.ordinality() > 1)
      updateTree->addProp("@selector-key", selectorAndKey.item(1));

   if (selectorAndAttrs.ordinality() > 1)
   {
      addUpdateAttributesFromString(updateTree, selectorAndAttrs.item(1));
   }

   index++;
   if (items.ordinality() == index) return;

   // children nodes
   IPropertyTree *children = updateTree->addPropTree("Children", createPTree("Children"));
   for ( unsigned i = index; i < items.ordinality() ; i++)
   {
       IPropertyTree *child = children->addPropTree("Child", createPTree("Child"));
       StringArray nameAndAttrs;
       nameAndAttrs.appendList(items.item(i), "@");
       child->addProp("@name", nameAndAttrs.item(0));
       if (nameAndAttrs.ordinality() > 1)
          addUpdateAttributesFromString(child, nameAndAttrs.item(1));
   }

}

void CEnvGen::addUpdateAttributesFromString(IPropertyTree *updateTree, const char *attrs)
{
   StringArray saAttrs;
   saAttrs.appendList(attrs, ATTR_SEP);
   //printf("attribute: %s\n",attrs);

   IPropertyTree *pAttrs = updateTree->addPropTree("Attributes", createPTree("Attributes"));
   for ( unsigned i = 0; i < saAttrs.ordinality() ; i++)
   {
     IPropertyTree *pAttr = pAttrs->addPropTree("Attribute", createPTree("Attribute"));

     StringArray keyValues;
     keyValues.appendList(saAttrs[i], "=");

     pAttr->addProp("@name", keyValues[0]);
     StringBuffer sbValue;
     sbValue.clear().appendf("%s", keyValues[1]);
     sbValue.replaceString("[equal]", "=");

     StringArray newOldValues;
     if (strcmp(keyValues[1], "") != 0)
     {
        newOldValues.appendList(sbValue.str(), ATTR_V_SEP);
        pAttr->addProp("@value", newOldValues[0]);
        if (newOldValues.ordinality() > 1) pAttr->addProp("@oldValue", newOldValues[1]);
     }
     else
        pAttr->addProp("@value", "");
   }
}

void CEnvGen::createUpdateNodeTask(const char* action, IPropertyTree * config, const char* param)
{
    StringBuffer sbParam;
    StringArray parts;
    parts.appendList(param, ":");

    String part1(parts.item(0));
    if (part1.startsWith("spark"))
    {
        sbParam.clear().append("pg:buildset#sparkthor");
        createUpdateTask(action, config, sbParam.str());
    }

    if (part1.startsWith("thor"))
    {
       for ( unsigned i = 1; i < parts.ordinality() ; i++)
       {
          sbParam.clear().append("sw:");
          StringBuffer sbPart1(part1.str());
          sbPart1.append(":instance-").appendf("%s",parts.item(i));
          sbParam.appendf("%s", sbPart1.str());
          createUpdateTask(action, config, sbParam.str());
       }
    }
    else
    {
       if (part1.startsWith("computer"))
           sbParam.append("hd:").appendf("%s", part1.str());

       else
       {
          sbParam.clear().append("sw:");
          StringBuffer sbPart1(part1.str());
          sbPart1.replaceString("@", ":instance@");
          sbParam.appendf("%s", sbPart1.str());
       }
       createUpdateTask(action, config, sbParam.str());
    }

}

void CEnvGen::createUpdateBindingTask(const char* action, IPropertyTree * config, const char* param)
{
    StringArray bindingAndAttrs;
    bindingAndAttrs.appendList(param, "@");
    StringArray espAndBinding;
    espAndBinding.appendList(bindingAndAttrs.item(0), ":");
    if (bindingAndAttrs.ordinality() != 2)
    {
       if (!stricmp(action, "modify") && (espAndBinding.ordinality() != 2))
          throw MakeStringException(CfgEnvErrorCode::InvalidParams,
             "\"-mod-binding\" should have format: <esp name>:<binding name>@attr1=<value>^attr2=value>");
       else if (!stricmp(action, "add"))
          throw MakeStringException(CfgEnvErrorCode::InvalidParams,
             "\"-add-binding\" should have format: <esp name>@name=<value>^service=<value>");
    }

    if (!stricmp(action, "remove") && (espAndBinding.ordinality() != 2))
        throw MakeStringException(CfgEnvErrorCode::InvalidParams,
           "\"-rmv-binding\" should have format: <esp name>:<binding name>");

    StringBuffer sbParam;
    sbParam.appendf("sw:esp#%s:EspBinding", espAndBinding.item(0));

    if ((espAndBinding.ordinality() == 2) && *(espAndBinding.item(1)))
    {
       if (stricmp(action, "remove"))
          sbParam.appendf("#%s", espAndBinding.item(1));
       else
          sbParam.appendf("@name=%s", espAndBinding.item(1));
    }

    if (stricmp(action, "remove"))
       sbParam.appendf("@%s", bindingAndAttrs.item(1));

    createUpdateTask(action, config, sbParam.str());
}

void CEnvGen::createUpdateServiceTask(const char* action, IPropertyTree * config, const char* param)
{
    StringBuffer sbParam;
    sbParam.append("sw:").appendf("%s", param);
    createUpdateTask(action, config, sbParam.str());
}

void CEnvGen::createUpdateTopologyTask(const char* action, IPropertyTree * config, const char* param)
{
    StringBuffer sbParam;
    if (!stricmp(action, "add") && !stricmp(param, "default"))
    {
        sbParam.clear().append("sw:topology#topology:cluster@name=thor:eclagent@process=myeclagent:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:thor@process=mythor");
        createUpdateTask(action, config, sbParam.str());

        sbParam.clear().append("sw:topology#topology:cluster@name=roxie:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:roxie@process=myroxie");
        createUpdateTask(action, config, sbParam.str());

        sbParam.clear().append("sw:topology#topology:cluster@name=thor_roxie:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:roxie@process=myroxie:thor@process=mythor");
        createUpdateTask(action, config, sbParam.str());
    }
    else
    {
        sbParam.clear().append("sw:topology#").appendf("%s", param);
        createUpdateTask(action, config, sbParam.str());
    }
}

bool CEnvGen::process()
{
   if (m_showInputOnly)
   {
      if (m_displayFormat == XML_Format)
      {
         StringBuffer cfgXML;
         toXML(m_params, cfgXML.clear());
         printf("Input as XML format:\n");
         printf("%s\n\n",cfgXML.str());
      }
      else if (m_displayFormat == JSON_Format)
      {
         StringBuffer cfgJSON;
         toJSON(m_params, cfgJSON.clear());
         printf("Input as JSON format:\n");
         printf("%s\n\n",cfgJSON.str());
      }
      else
         printf("Unknown display format: %d \n\n", m_displayFormat);

      return true;
   }

   IPropertyTree* config = m_params->queryPropTree("Config");
   const char* mode = config->queryProp("@mode");
   if (!stricmp(mode, "create") )
      m_iConfigEnv->create(config);
   else
      m_iConfigEnv->runUpdateTasks(config);


   // blindly set attribute with xpath
   if (m_arrXPaths.length() > 0)
   {
      unsigned nCount = 0;
      while (nCount < m_arrXPaths.length())
      {
         m_iConfigEnv->setAttribute( m_arrXPaths.item(nCount).str(),
                                     m_arrAttrib.item(nCount).str(),
                                     m_arrValues.item(nCount).str()
                                 );
         nCount++;
      }
   }


   // blindly add content with xpath
   if (m_arrContentXPaths.length() > 0)
   {
      unsigned nCount = 0;
      while (nCount < m_arrContentXPaths.length())
      {
         int format = (!stricmp(m_arrContentFormats.item(nCount), "json")) ? JSON_Format : XML_Format;
         m_iConfigEnv->addContent( m_arrContentXPaths.item(nCount),
                                   m_arrContents.item(nCount), format);
         nCount++;
      }
   }

   StringBuffer out;
   m_iConfigEnv->getContent(NULL, out, XML_SortTags | XML_Format);

   Owned<IFile> pFile;
   const char* envFile = config->queryProp("@env-out");

   //printf("output envxml to file %s\n", envFile);

   pFile.setown(createIFile(envFile));

   Owned<IFileIO> pFileIO;
   pFileIO.setown(pFile->open(IFOcreaterw));
   pFileIO->write(0, out.length(), out.str());

   //printf("%s", out.str());

   if (!m_iConfigEnv->isEnvironmentValid(out))
   {
      fprintf(stderr, "The result environment.xml is invalid.\n");
      return false;
   }

   return true;
}

void CEnvGen::usage()
{
  const char* version = "0.1";
  printf("\nHPCC Systems® environment generator. version %s. Usage:\n", version);
  puts("   envgen2 -env-out <environment file> -ip <ip addr> [options]");
  puts("");
  puts("options: ");
  puts("   -env-in : Full path of the input environment file to update.");
  puts("   -env-out | -env: Full path of the output environment file to generate or update.");
  puts("          If a file with the same name exists, and no \"-update\" provided");
  puts("          a new name with _xxx will be generated ");
  puts("   -ip  : Ip addresses that should be used for environment generation");
  puts("          Allowed formats are ");
  puts("          X.X.X.X;");
  puts("          X.X.X.X-XXX;");
  puts("   -ipfile: name of the file that contains Ip addresses");
  puts("          Allowed formats are ");
  puts("          X.X.X.X;");
  puts("          X.X.X.X-XXX;");
  puts("   -supportnodes <number of support nodes>: Number of nodes to be used");
  puts("           for non-Thor and non-Roxie components. If not specified or ");
  puts("           specified as 0, Thor and Roxie nodes may overlap with support");
  puts("           nodes. If an invalid value is provided, support nodes are ");
  puts("           treated as 0");
  puts("   -roxienodes <number of roxie nodes>: Number of nodes to be generated ");
  puts("          for Roxie. If not specified or specified as 0, no roxie nodes");
  puts("          are generated");
  puts("   -thornodes <number of thor nodes>: Number of nodes to be generated ");
  puts("          for Thor slaves. A node for thor master is automatically added. ");
  puts("          If not specified or specified as 0, no thor nodes");
  puts("   -espnodes <number of esp nodes>: Number of nodes to be generated  ");
  puts("          If not specified a single ESP node is generated");
  puts("          If 0 is specified then all available nodes specified with assign_ips flag ");
  puts("          will be used, otherwise 0 ESP nodes will be generated.");
  puts("   -slavesPerNode <number of thor slaves per node>: Number of thor nodes ");
  puts("          per slave.");
  puts("   -thorChannelsPerSlave <number of channels per thor slave>: Number of thor channels per slave.");
  puts("          The default is 1.");
  puts("   -roxieChannelsPerSlave <number of channels per roxie slave>: Number of roxie channels per slave.");
  puts("          The default is 1.");
  puts("   -roxieondemand <enable roxie on demand(1) or disable roxie on demand(any ");
  puts("          other value)>: Specify 1 to enable Roxie on demand. ");
  puts("          Any other value will disable Roxie on demand");
  puts("   -o <categoryname=newdirvalue>: overrides for any common directories");
  puts("          There can be multiple -o options. Each override should still");
  puts("          contain a [NAME] and either a [CATEGORY] or [INST]. ");
  puts("          If a category already exists, the directory value is updated. Otherwise");
  puts("          a new category is created.");
  puts("          For example, \"-o log=/var/logs/[NAME]/mylogs/[INST] -o run=/var/run/[NAME]/myrun/[INST]\"");
  puts("   -override <buildset,xpath,value>: overrides all component properties with the");
  puts("          given xpath for the given buildset with the value provided. If the xpath is");
  puts("          not already present, it is added to any of the components");
  puts("          There can be multiple of the -override options. For example, to override the dropzone");
  puts("          directory and to set eclwatch's enableSystemUseRewrite to true, provide the following options:");
  puts("             \"-override DropZone,@directory,/mnt/disk1/mydropzone ");
  puts("               -override espsmc,@enableSystemUseRewrite,true\"");
  puts("   -set-xpath-attrib-value <XPATH> <ATTRIBUTE> <VALUE>: sets or add the xpath attribute and value.");
  puts("          Example: \"-set-xpath-attrib-value  Software/Topology/Cluster[@name=\"thor\"]/ThorCluster @process thor123\"");
  puts("   -add-content <XPATH> <content file in XML or JSON format>: adds  content to the xpath.");
  puts("          Example: \"-add-content  Software/Topology /tmp/new_topology_cluster.xml");
  puts("   -env-options environment options file. The default is /etc/HPCCSystems/environment.conf.");
  puts("   -env-rules: a rule file to generate environment. The default is /etc/HPCCSystems/genenvrules.conf.");
  puts("   -env-rules: a rule file to generate environment. The default is /etc/HPCCSystems/genenvrules.conf.");
  puts("   -buildset: the buildset file with full path. The directory also contains all XSD files.");
  puts("              the default is /opt/HPCCSystems/componentfiles/configxml/buildset.xml.");
  puts("   -cloud <none|aws>: set certain attributes for particular cloud environment. The default is none");

  puts("");
  puts("For update, there are three input formats. See following for the usage.");
  puts("   -help-update-1: show common update options");
  puts("   -help-update-2 (for internal use): show more general one-line update options");
  puts("   -help-update-3 (for internal use): show update options in xml format");
  puts("   -help-update-3-json (for internal use): show update options in json format");
  puts("");
  puts("   -show-input-only: show input parameters in xml format. Doesn't create/update environment.");
  puts("   -show-input-json-only: show input parameters in json format. Doesn't create/update environment.");
  puts("   -help: print out this usage.");
  puts("");
}

void CEnvGen::usage_update_input_format_1()
{

  //new options
  puts("");
  puts("These options are for basic environment operations.");
  puts("   -add-node comp#name@(ip|ipfile)=<value>  for ip <value> can be <value1>;<value2>...");
  puts("   -mod-node \"comp#name@ip=<new>\\|<old>\"");
  puts("   -rmv-node comp#name@ip=<value>");
  puts("For examples:");
  puts("   -add-node thor#name:master@ip=10.20.10.10:slave@ipfile=/tmp/slave_ip_list");
  puts("   -mod-node \"thor#name:master@ip=1.10.12.8\\|1.20.10.10\"");
  puts("   -rmv-node \"thor#name:slave@ip=10.20.2.10\\|10.20.12.2\"");
  puts("   -add-node \"roxie#newroxie\\|myroxie@ip=1.20.20.10-11");
  puts("");
  puts("   -add-service service#name@buildSet=<value>");
  puts("For examples:");
  puts("   -add-service esdl#dynamicesdl@buildSet=DynamicESDL");
  puts("");
  puts("   -add-binding <esp name>@name=<binding name>^service=<service name>[^port=<value>]");
  puts("   -mod-binding <esp name>:<binding name>@<attr>=<value>");
  puts("   -rmv-binding <esp name>:<binding name>");
  puts("For examples:");
  puts("   -add-binding myesp@name=mydesdl^service=dynamicesdl^port=8043");
  puts("   -mod-binding myesp:mydesdl@port=8050");
  puts("   -rmv-binding myesp:mydesdl");
  puts("");
  puts("   -add-topology name|clone:cluster@name=<value>:roxie@process=<value>:eclcc@process=<value>:eclegent@process=<value> ");
  puts("   To add default topology: -add-topology default");
  puts("   -mod-topology <topology name>:<cluster name>:eclagent@process=<value>");
  puts("   -rmv-topology <topology name>:cluster#name>:eclagent@process=<value>");
  puts("For examples:");
  puts("   -add-topology topology:cluster@name=cluster2:roxie@process=roxie2:eclcc@process=eclcc2:eclagent@process=agent2 ");
  puts("");
}

void CEnvGen::usage_update_input_format_2()
{
// add xxx-binding and xxx-service
  puts("");
  puts("These options are for updating environment with one line format.");
  puts("   -add category(hd:sw:pg):comp(esp|computer)#name[:selector(instance|dir|<relative xpath>)]@attr=value");
  puts("   -mod category(hd:sw:pg):comp(esp|computer)#name[:selelctor(instance|dir|<xpath>)]@attr1=value|old");
  puts("   -rmv category(hd:sw:pg):comp@name(service@ws_ecl|computer@localhost):target<xpath to delete>:selector<condition>(instance|dir|<relative xpath>):attr=value|old");
  puts("For examples:");
  puts("   -add sw:roxie#myroxie:instance@ip=1.0.0.22;1.0.0.23");
  puts("   -mod sw:dali#mydali:instance@ip=1.0.0.20");
  puts("   -mod sw:thor#mythor@channelsPerSlave=4");
  puts("   -mod sw:backupnode#mybackupnode:NodeGroup@name=thor2");
  puts("   -rmv hd:computer@ip=1.1.0.30");
  puts("");
}

void CEnvGen::usage_update_input_format_3()
{
  puts("");
  puts("This option is for updating environment with in either XML format.");
  puts("   -in-file <input-file-name>.xml");
  puts("      format:");
  puts("<Env>");
  puts("  <Config");
  puts("    env-in=[input environment file]");
  puts("    env-out|env=[output environment file]");
  puts("    mode=[create|update]");
  puts("    ip-list=[single ip or ip list separated by ;]");
  puts("    ip-file=[ip file with one ip per line end with ;]");
  puts("    support-nodes=[num of support nodes]");
  puts("    esp-nodes=[num of esp nodes]");
  puts("    roxie-nodes=[num of roxie nodes]");
  puts("    thor-nodes=[num of thor nodes]");
  puts("    slaves-per-node=[num of slaves per node]");
  puts("    roxie-on-demand=[true|false]");
  puts("    options=[environment.conf full path]");
  puts("    rules=[genrules.conf full path]");
  puts("    buildset=[buidset.xml full path]");
  puts("  >");
  puts("    <Task category=[hardware|software|programs|envsettings]");
  puts("          component=[computer|dali|esp|roxie|thor|topology|service|...]");
  puts("          key=[component name list separated by \",\"");
  puts("          operation=[add|modify|remove]");
  puts("          target=[node|attribure|child]  <!- These are remove target. The default is node -->");
  puts("          selector=[xpath] <!- xpath of node to apply the operation. Such as \"instance\" -->");
  puts("          selector-key=[selector unique attribute]");
  puts("          id=[node id]   <!- with this no category, component, key, selector and selector-key are needed -->");
  puts("     >");
  puts("       <Attributes>");
  puts("         <Attribute name=[attribute name] value=[attribute value] oldValue=[old value]/>");
  puts("         ......");
  puts("       </Attributes>");
  puts("       <Children>");
  puts("         <Child name=[value]>");
  puts("           <Attributes>");
  puts("             <Attribute name=[attribute name] value=[attribute value] oldValue=[old value]/>");
  puts("             ......");
  puts("           </Attributes>");
  puts("         </Child>");
  puts("         ......");
  puts("       </Children>");
  puts("     </Task>");
  puts("</Config></Env>");
  puts("For example:");
  puts("  <Env>");
  puts("    <Config env-in=\"/tmp/environment_in.xml\" env-out=\"/tmp/environment_out.xml\" mode=\"update\">");
  puts("      <Task category=\"Hardware\" component=\"computer\" operation=\"modify\">");
  puts("        <Attributes>");
  puts("          <Attribute name=\"ip\" oldValue=\"1.0.0.30\" value=\"1.1.0.50\"/>");
  puts("        </Attributes>");
  puts("      </Task>");
  puts("    </Config>");
  puts("  </Env>");
  puts("");

}

void CEnvGen::usage_update_input_format_3_json()
{
  puts("");
  puts("This option is for updating environment with in either JSON format.");
  puts("   -in-file <input-file-name>.json");
  puts("      For exmaple:");
  puts("{");
  puts(" \"Config\": {");
  puts("    \"@env-in\": \".\\/src\\/templates\\/env_base.xml\",");
  puts("    \"@env-out\": \"\\/tmp\\/envgen2_test\\/roxie_add_ip_1.xml\",");
  puts("    \"@mode\": \"update\",");
  puts("    \"Task\": {");
  puts("        \"@category\": \"Software\",");
  puts("        \"@component\": \"roxie\",");
  puts("        \"@key\": \"myroxie\",");
  puts("        \"@operation\": \"add\",");
  puts("        \"@selector\": \"instance\",");
  puts("        \"Attributes\": {");
  puts("           \"Attribute\": {");
  puts("              \"@name\": \"ip\",");
  puts("              \"@value\": \"1.0.0.22\"");
  puts("            }");
  puts("          }");
  puts("      }");
  puts("  }");
  puts("}");
  puts("");
}


void CEnvGen::addUpdateTaskFromFile(const char * inFile)
{
   Owned<IPropertyTree> inPTree;

   if ((String(inFile).toLowerCase())->endsWith(".json"))
   {
      StringBuffer sbFile;
      sbFile.loadFile(inFile);
      inPTree.setown(createPTreeFromJSONString(sbFile.str()));
   }
   else
   {
       inPTree.setown(createPTreeFromXMLFile(inFile));
   }

   // add Config attributies to params
   IPropertyTree *pCfg = m_params->queryPropTree("Config");
   assert(pCfg);
   Owned<IAttributeIterator> attrIter = inPTree->getAttributes();
   ForEach(*attrIter)
   {
      const char* propName = attrIter->queryName();
      if (!(*propName)) continue;
      pCfg->setProp(propName, attrIter->queryValue());

   }

   // add Tasks to params
   Owned<IPropertyTreeIterator> taskIter = inPTree->getElements("Task");
   ForEach(*taskIter)
   {
      IPropertyTree* task = &taskIter->query();
      StringBuffer sb;
      toXML(task, sb);
      pCfg->addPropTree("Task", createPTreeFromXMLString(sb.str()));

   }

}

bool CEnvGen::convertOverrideTask(IPropertyTree *config, const char * input)
{
   StringArray sbarr;
   sbarr.appendList(input, ",");

   StringArray sbarrXPath;
   sbarrXPath.appendList(sbarr.item(1), "@");

   StringBuffer sbTask;
   sbTask.clear().appendf("sw:%s", sbarr.item(0));
   if (sbarrXPath.ordinality() < 2)
   {
      fprintf(stderr, "Override xpath miss '@'\n");
      return false;
   }
   if ((sbarrXPath.ordinality() == 2) && !(*sbarrXPath.item(0)))
       sbTask.appendf("@%s=%s", sbarrXPath.item(1), sbarr.item(2));
   else
   {
      sbTask.appendf(":selector=%s", sbarrXPath.item(0));
      for (unsigned i=1; i < sbarrXPath.ordinality()-1; i++)
         sbTask.appendf("#%s", sbarrXPath.item(i));
      sbTask.appendf("@%s=%s", sbarrXPath.item(sbarrXPath.ordinality()-1), sbarr.item(2));
   }

   createUpdateTask("modify", config, sbTask.str());
   return true;
}

void CEnvGen::cloudConfiguration(IPropertyTree * config, const char* cloud)
{
   if (!stricmp(cloud, "aws"))
   {
       createUpdateTask("modify", config, "sw:espsmc@enableSystemUseRewrite=true|false");
       createUpdateTask("modify", config, "sw:roxie@roxieMulticastEnabled=false");
   }
}


int main(int argc, char** argv)
{

   InitModuleObjects();

   //CEnvGen * envGen = new CEnvGen();
   CEnvGen  envGen;

   try {
      if (!envGen.parseArgs(argc, argv))
      {
   //      delete envGen;
         return 1;
      }
      envGen.process();

   }
   catch (IException* e)
   {
     int errCode = e->errorCode();
     StringBuffer errMsg;
     e->errorMessage(errMsg);
     printf("Error: %d, %s\n", errCode, errMsg.str());
     e->Release();
    // delete envGen;
     return 1;
   }
   //delete envGen;
   return 0;
}
