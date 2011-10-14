/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#include "jliball.hpp"
#include "XMLTags.h"
#include "deployutils.hpp"
#include "build-config.h"

#define ENVGEN_PATH_TO_ESP_CONFIG INSTALL_DIR""CONFIG_DIR"/configmgr/esp.xml"
#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml/"
#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"

void usage()
{
  const char* version = "1.1";
  printf("HPCC Systems environment generator. version %s. Usage:\n", version);
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
  puts("   -help: print out this usage.");
}

int main(int argc, char** argv)
{
  InitModuleObjects();

  const char* out_envname = NULL;
  const char* in_ipfilename;
  StringBuffer ipAddrs;
  int roxieNodes=0, thorNodes=0, slavesPerNode=1;
  bool roxieOnDemand = true;
  MapStringTo<StringBuffer> dirMap;

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
    else if (stricmp(argv[i], "-env") == 0)
    {
      i++;
      out_envname = argv[i++];
    }
    else if (stricmp(argv[i], "-roxienodes") == 0)
    {
      i++;
      roxieNodes = atoi(argv[i++]);
    }
    else if (stricmp(argv[i], "-thornodes") == 0)
    {
      i++;
      thorNodes = atoi(argv[i++]);
    }
    else if (stricmp(argv[i], "-slavespernode") == 0)
    {
      i++;
      slavesPerNode = atoi(argv[i++]);
    }
    else if (stricmp(argv[i], "-roxieondemand") == 0)
    {
      i++;

      if (atoi(argv[i++]) != 1)
        roxieOnDemand = false;
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
      DelimToStringArray(argv[i++], sbarr, "=");
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

    optionsXml.appendf("<XmlArgs roxieNodes=\"%d\" thorNodes=\"%d\" slavesPerNode=\"%d\" roxieOnDemand=\"%s\" ipList=\"%s\"/>", roxieNodes,
      thorNodes, slavesPerNode, roxieOnDemand?"true":"false", ipAddrs.str());

    buildEnvFromWizard(optionsXml, pServiceName, pCfg, envXml, &dirMap);
    if(envXml.length())
    {
      StringBuffer env;
      StringBuffer thisip;
      queryHostIP().getIpText(thisip);
      env.appendf("<"XML_HEADER">\n<!-- Generated with envgen on ip %s -->\n", thisip.str());
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

