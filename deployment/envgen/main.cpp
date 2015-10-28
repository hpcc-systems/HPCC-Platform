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
#include "jliball.hpp"
#include "XMLTags.h"
#include "deployutils.hpp"
#include "build-config.h"

#define ENVGEN_PATH_TO_ESP_CONFIG INSTALL_DIR "" CONFIG_DIR "/configmgr/esp.xml"
#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml/"
#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"

bool validateInteger(const char *str,int &out)
{
  bool bIsValid = false;
  char *end = NULL;

  errno = 0;

  const long sl = strtol(str,&end,10);

  if (end == str)
  {
    fprintf(stderr, "Error: non-integer parameter '%s' specified.\n",str);
  }
  else if ('\0' != *end)
  {
    fprintf(stderr, "Error: non-integer characters found in '%s' when expecting integer input.\n",str);
  }
  else if ( (INT_MAX < sl || INT_MIN > sl) || ((LONG_MIN == sl || LONG_MAX == sl) && ERANGE == errno))
  {
    fprintf(stderr, "Error: integer '%s' is out of range.\n",str);
  }
  else
  {
    out = (int)sl;
    bIsValid = true;
  }

  return bIsValid;
}

void usage()
{
  const char* version = "1.1";
  printf("HPCC Systems® environment generator. version %s. Usage:\n", version);
  puts("   envgen -env <environment file> -ip <ip addr> [options]");
  puts("");
  puts("options: ");
  puts("   -env : Full path of the environment file that will be generated.");
  puts("          If a file with the same name exists, a new name with _xxx ");
  puts("          will be generated ");
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
  puts("           specified as 0, thor and roxie nodes may overlap with support");
  puts("           nodes. If an invalid value is provided, support nodes are ");
  puts("           treated to be 0");
  puts("   -roxienodes <number of roxie nodes>: Number of nodes to be generated ");
  puts("          for roxie. If not specified or specified as 0, no roxie nodes");
  puts("          are generated");
  puts("   -thornodes <number of thor nodes>: Number of nodes to be generated ");
  puts("          for thor slaves. A node for thor master is automatically added. ");
  puts("          If not specified or specified as 0, no thor nodes");
  puts("          are generated");
  puts("   -slavesPerNode <number of thor slaves per node>: Number of thor nodes ");
  puts("          per slave.");
  puts("   -roxieondemand <enable roxie on demand(1) or disable roxie on demand(any ");
  puts("          other value)>: Enable roxie on demand by specifying 1 for flag. ");
  puts("          Any other value will disable roxie on demand");
  puts("   -o <categoryname=newdirvalue>: overrides for any common directories");
  puts("          There can be multiple of the -o options. Each override should still");
  puts("          contain a [NAME] and either a [CATEGORY] or [INST]. ");
  puts("          If category already exists, the directory value is updated. Otherwise");
  puts("          a new category is created.");
  puts("          For example, \"-o log=/var/logs/[NAME]/mylogs/[INST] -o run=/var/run/[NAME]/myrun/[INST]\"");
  puts("   -override <buildset,xpath,value>: overrides all component properties with the");
  puts("          given xpath for the given buildset with the provided value. If the xpath is");
  puts("          not already present, it is added to any of the components");
  puts("          There can be multiple of the -override options. For example, to override the dropzone");
  puts("          directory and to set eclwatch's enableSystemUseRewrite to true, the following options");
  puts("          can be provided.");
  puts("          \"-override DropZone,@directory,/mnt/disk1/mydropzone ");
  puts("          -override espsmc,@enableSystemUseRewrite,true\"");
  puts("   -set_xpath_attrib_value <XPATH> <ATTRIBUTE> <VALUE>: sets or add the xpath with attribute and value.");
  puts("          Example: \"-set_xpath_attrib_value  Software/Topology/Cluster[@name=\"thor\"]/ThorCluster @process thor123\"");
  puts("   -help: print out this usage.");
}

int main(int argc, char** argv)
{
  InitModuleObjects();

  const char* out_envname = NULL;
  const char* in_ipfilename;
  StringBuffer ipAddrs;
  int roxieNodes=0, thorNodes=0, slavesPerNode=1, supportNodes=0;
  bool roxieOnDemand = true;
  MapStringTo<StringBuffer> dirMap;
  StringArray overrides;
  StringBufferArray arrXPaths;
  StringBufferArray arrAttrib;
  StringBufferArray arrValues;

  int i = 1;
  bool writeToFiles = false;
  int port = 80;

  while(i<argc)
  {
    if(stricmp(argv[i], "-help") == 0 || stricmp(argv[i], "-?") == 0)
    {
      usage();
      releaseAtoms();
      return 0;
    }
    else if (stricmp(argv[i], "-set_xpath_attrib_value")== 0)
    {
        i++;
        arrXPaths.append(*new StringBufferItem (argv[i++]));
        arrAttrib.append(*new StringBufferItem (argv[i++]));
        arrValues.append(*new StringBufferItem (argv[i++]));
    }
    else if (stricmp(argv[i], "-env") == 0)
    {
      i++;
      out_envname = argv[i++];
    }
    else if (stricmp(argv[i], "-supportnodes") == 0)
    {
      i++;

      if (validateInteger(argv[i++],supportNodes) != true)
      {
        releaseAtoms();
        return 1;
      }
    }
    else if (stricmp(argv[i], "-roxienodes") == 0)
    {
      i++;

      if (validateInteger(argv[i++],roxieNodes) != true)
      {
        releaseAtoms();
        return 1;
      }
    }
    else if (stricmp(argv[i], "-thornodes") == 0)
    {
      i++;

      if (validateInteger(argv[i++],thorNodes) != true)
      {
        releaseAtoms();
        return 1;
      }
    }
    else if (stricmp(argv[i], "-slavespernode") == 0)
    {
      i++;

      if (validateInteger(argv[i++],slavesPerNode) != true)
      {
        releaseAtoms();
        return 1;
      }
    }
    else if (stricmp(argv[i], "-roxieondemand") == 0)
    {
      i++;
      int iConverted = 0;

      if (validateInteger(argv[i++],iConverted) != true)
      {
        releaseAtoms();
        return 1;
      }

      if (iConverted != 1)
      {
        roxieOnDemand = false;
      }
    }
    else if (stricmp(argv[i], "-ip") == 0)
    {
      i++;
      ipAddrs.append(argv[i++]);
    }
    else if(stricmp(argv[i], "-ipfile") == 0)
    {
      i++;
      in_ipfilename = argv[i++];
      ipAddrs.loadFile(in_ipfilename);
    }
    else if(stricmp(argv[i], "-o") == 0)
    {
      i++;
      StringArray sbarr;
      sbarr.appendList(argv[i++], "=");
      if (sbarr.length() != 2)
       continue;

      if (strstr(sbarr.item(1), "[NAME]") && (strstr(sbarr.item(1), "[INST]") || strstr(sbarr.item(1), "[COMPONENT]")))
        dirMap.setValue(sbarr.item(0), sbarr.item(1));
      else
      {
        fprintf(stderr, "Error: Directory Override must contain [NAME] and either [INST] or [COMPONENT]\n");
        releaseAtoms();
        return 1;
      }
    }
    else if(stricmp(argv[i], "-override") == 0)
    {
      i++;
      overrides.append(argv[i++]);
    }
    else
    {
      fprintf(stderr, "Error: unknown command line parameter: %s\n", argv[i]);
      usage();
      releaseAtoms();
      return 1;
    }
  }

  if (!out_envname)
  {
    fprintf(stderr, "Error: Output environment xml file is required. Please specify.\n");
    usage();
    releaseAtoms();
    return 1;
  }

  if (ipAddrs.length() == 0)
  {
    fprintf(stderr, "Error: Ip addresses are required. Please specify.\n");
    usage();
    releaseAtoms();
    return 1;
  }

  try
  {
    validateIPS(ipAddrs.str());
    StringBuffer optionsXml, envXml;
    const char* pServiceName = "WsDeploy_wsdeploy_esp";
    Owned<IPropertyTree> pCfg = createPTreeFromXMLFile(ENVGEN_PATH_TO_ESP_CONFIG);

    optionsXml.appendf("<XmlArgs supportNodes=\"%d\" roxieNodes=\"%d\" thorNodes=\"%d\" slavesPerNode=\"%d\" roxieOnDemand=\"%s\" ipList=\"%s\"/>", supportNodes, roxieNodes,
      thorNodes, slavesPerNode, roxieOnDemand?"true":"false", ipAddrs.str());

    buildEnvFromWizard(optionsXml, pServiceName, pCfg, envXml, &dirMap);

    if(envXml.length())
    {
      if (overrides.length() || arrXPaths.length())
      {
        Owned<IPropertyTree> pEnvTree = createPTreeFromXMLString(envXml.str());

        for(unsigned i = 0; i < overrides.ordinality() ; i++)
        {
          StringArray sbarr;
          sbarr.appendList(overrides.item(i), ",");

          if (sbarr.length() != 3)
          {
            fprintf(stderr, "\nWarning: unable to override %s as override option needs 3 valid values to override.\n", overrides.item(i));
            continue;
          }

          const char* buildset = sbarr.item(0);
          bool flag = false;

          if (buildset && *buildset)
          {
            StringBuffer xpath(XML_TAG_SOFTWARE"/");
            xpath.appendf("*[" XML_ATTR_BUILDSET "='%s']", buildset);
            Owned<IPropertyTreeIterator> iter = pEnvTree->getElements(xpath.str());

            ForEach (*iter)
            {
              flag = true;
              IPropertyTree* pComp = &iter->query();
              const char* prop = sbarr.item(1);

              if (prop && *prop)
                pComp->setProp(prop, sbarr.item(2));
            }
          }

          if (!buildset || !*buildset || !flag)
            fprintf(stderr, "\nWarning: unable to find components of buildset '%s' for override option '%s'.\n", buildset?buildset:"", overrides.item(i));
        }

        if (arrXPaths.length() > 0)
        {
            int nCount = 0;

            while (nCount < arrXPaths.length())
            {
                IPropertyTree *attrTree = pEnvTree->queryPropTree(arrXPaths.item(nCount).str());

                if (attrTree == NULL)
                {
                    attrTree = createPTree();
                    attrTree->appendProp(arrAttrib.item(nCount).str(), arrValues.item(nCount).str());
                    pEnvTree->setPropTree(arrXPaths.item(nCount).str(), attrTree);
                }
                else
                    attrTree->setProp(arrAttrib.item(nCount).str(), arrValues.item(nCount).str());

                nCount++;
            }
        }
        toXML(pEnvTree, envXml.clear());
      }


      StringBuffer env;
      StringBuffer thisip;
      queryHostIP().getIpText(thisip);
      env.appendf("<" XML_HEADER ">\n<!-- Generated with envgen on ip %s -->\n", thisip.str());
      env.append(envXml);
      
      Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
      Owned<IConstEnvironment>  constEnv = factory->loadLocalEnvironment(env);
      validateEnv(constEnv);

      Owned<IFile> pFile;
      pFile.setown(createIFile(out_envname));
      
      Owned<IFileIO> pFileIO;
      pFileIO.setown(pFile->open(IFOcreaterw));
      pFileIO->write(0, env.length(), env.str());
    }
  }
  catch(IException *excpt)
  {
    StringBuffer errMsg;
    fprintf(stderr, "Exception: %d:\n%s\n", excpt->errorCode(), excpt->errorMessage(errMsg).str());
    releaseAtoms();
    excpt->Release();
    return 1;
  }
  catch(...)
  {
    fprintf(stderr, "Unknown exception\n");
    releaseAtoms();
    return 1;
  }

  releaseAtoms();

  return 0;
}

