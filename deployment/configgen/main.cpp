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
#include "configengcallback.hpp"
#include "deploy.hpp"
#include "build-config.h"

#define STANDARD_INDIR COMPONENTFILES_DIR"/configxml"
#define STANDARD_OUTDIR RUNTIME_DIR

void usage()
{
  const char* version = "1.1";
  printf("HPCC Systems configuration generator. version %s. Usage:\n", version);
  puts("   configgen -env <environment file> -ip <ip addr> [options]");
  puts("");
  puts("options: ");
  puts("   -env : The configuration environment to be parsed.");
  puts("   -ip  : The ip address that will be matched against a component ");
  puts("          instance's ip address. If matched, configuration files are ");
  puts("          generated for that component");
  puts("   -c <component name>: Optional component name for which the ");
  puts("          configuration is generated. If -t is also specified, it is ");
  puts("          ignored");
  puts("   -t <component type>: Optional component type for which the ");
  puts("          configuration is generated. If -c is also specified, the ");
  puts("          component name is used");
  puts("   -id <input directory>: The input directory for the supporting ");
  puts("          xml environment files like xsd's, xsl's and ");
  puts("          configgencomplist.xml. If not specified, the following ");
  puts("          defaults are used. ");
  puts("          For win32, 'c:\\trunk\\initfiles\\componentfiles\\configxml'");
  puts("          For Linux, '"COMPONENTFILES_DIR"/configxml/'");
  puts("   -od <output directory>: The output directory for the generated files.");
  puts("          If not specified, the following defaults are used. ");
  puts("          For win32, '.'");
  puts("          For Linux, '"CONFIG_DIR"'");
  puts("   -list: Lists out the components for a specific ip in the format");
  puts("          componentType=componentName;config file directory. Does not ");
  puts("          generate any output files. If masters and slaves exist for ");
  puts("          a component like Roxie or thor, then only the master entry ");
  puts("          is returned. ");
  puts("   -listall: Lists out all the components specified in the environment");
  puts("          that have an instance defined. Does not require an ip. Does ");
  puts("          not generate any output files. Output is written to stdout ");
  puts("          in the csv format as follows");
  puts("          ProcessType,componentNameinstanceip,instanceport,runtimedir,logdir");
  puts("          Missing fields will be empty.");
  puts("   -listdirs: Lists out any directories that need to be created during ");
  puts("          init time. Currently, directories for any drop zones ");
  puts("          with the same ip as the -ip option are returned. Format is ");
  puts("          one directory per line.");
  puts("   -listcommondirs: Lists out all directories that are listed under ");
  puts("          Software/Directories section in the following format. ");
  puts("          <CategoryName>=<DirectoryValue>");
  puts("          Each directory will be listed on a new line.");
  puts("   -machines: Lists out all names or ips of machines specified in the environment");
  puts("          Output is written to stdout, one machine per line.");
  puts("   -validateonly: Validates the environment, without generating permanent ");
  puts("          configurations. Returns 0 if environment is valid and non zero ");
  puts("          in other cases. Validation errors are printed to stderr.");
  puts("          Ignores -od flag, if supplied.");
  puts("   -v   : Print verbose output to stdout");
  puts("   -help: print out this usage.");
}

void deleteRecursive(const char* path)
{
    Owned<IFile> pDir = createIFile(path);
    if (pDir->exists())
    {
        if (pDir->isDirectory())
        {
            Owned<IDirectoryIterator> it = pDir->directoryFiles(NULL, false, true);
            ForEach(*it)
            {               
                StringBuffer name;
                it->getName(name);
                
                StringBuffer childPath(path);
                childPath.append(PATHSEPCHAR);
                childPath.append(name);
                
                deleteRecursive(childPath.str());
            }
        }
        pDir->remove();
    }
}

//returns temp path that ends with path sep
//
#ifdef _WIN32
extern DWORD getLastError() { return ::GetLastError(); }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
  ::GetTempPath(bufsize, tempPath);
  ::GetLongPathName(tempPath, tempPath, bufsize);
  if (subdir && *subdir)
  {
    const int len = strlen(tempPath);
    char* p = tempPath + len;
    strcpy(p, subdir);
    p += strlen(subdir);
    *p++ = '\\';
    *p = '\0';
  }
}
#else//Linux specifics follow
extern DWORD getLastError() { return errno; }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
  assert(bufsize > 5);
  strcpy(tempPath, "/tmp/");
  if (subdir && *subdir)
  {
    strcat(tempPath, subdir);
    strcat(tempPath, "/");
  }
}
#endif

void replaceDotWithHostIp(IPropertyTree* pTree, bool verbose)
{
    StringBuffer ip;
    queryHostIP().getIpText(ip);
    const char* attrs[] = {"@netAddress", "@roxieAddress", "@daliAddress"};
    StringBuffer xPath;

    for (int i = 0; i < sizeof(attrs)/sizeof(char*); i++)
    {
        xPath.clear().appendf(".//*[%s]", attrs[i]);
        Owned<IPropertyTreeIterator> iter = pTree->getElements(xPath.str());

        ForEach(*iter)
        {
            IPropertyTree* pComponent = &iter->query();
            Owned<IAttributeIterator> iAttr = pComponent->getAttributes();

            ForEach(*iAttr)
            {
                const char* attrName = iAttr->queryName();

                if (!strcmp(attrName, attrs[i]))
                {
                    String sAttrVal(iAttr->queryValue());
                    String dot(".");
                    
                    if (sAttrVal.equals(dot) || sAttrVal.indexOf(".:") == 0 || sAttrVal.indexOf("http://.:") == 0)
                    {
                        StringBuffer sb(sAttrVal);
                        if (sAttrVal.equals(dot))
                            sb.replaceString(".", ip.str());
                        else
                        {
                            ip.append(":");
                            sb.replaceString(".:", ip.str());
                            ip.remove(ip.length() - 1, 1);
                        }
                        
                        pComponent->setProp(attrName, sb.str());

                        if (verbose)
                            fprintf(stdout, "Replacing '.' with host ip '%s' for component/attribute '%s'[@'%s']\n", ip.str(), pComponent->queryName(), attrName);
                    }
                }
            }    
        }
    }
}

int processRequest(const char* in_cfgname, const char* out_dirname, const char* in_dirname, 
                   const char* compName, const char* compType, const char* in_filename, 
                   const char* out_filename, bool generateOutput, const char* ipAddr, 
                   bool listComps, bool verbose, bool listallComps, bool listdirs, 
                   bool listcommondirs, bool listMachines, bool validateOnly) 
{
  Owned<IPropertyTree> pEnv = createPTreeFromXMLFile(in_cfgname);
  short nodeIndex = 1;
  short index = 1;
  short compTypeIndex = 0;
  short buildSetIndex = 0;
  StringBuffer lastCompAdded;
  StringBuffer xPath("*");
  CConfigEngCallback callback(verbose);
  Owned<IPropertyTreeIterator> iter = pEnv->getElements(xPath.str());
  Owned<IConstEnvironment> m_pConstEnvironment;
  Owned<IEnvironment>      m_pEnvironment;

  replaceDotWithHostIp(pEnv, verbose); 
  StringBuffer envXML;
  toXML(pEnv, envXML);

  Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
  m_pEnvironment.setown(factory->loadLocalEnvironment(envXML));
  m_pConstEnvironment.set(m_pEnvironment);

  if (validateOnly)
  {
    char tempdir[_MAX_PATH];
    StringBuffer sb;

    while(true)
    {
      sb.clear().appendf("%d", msTick());
      getTempPath(tempdir, sizeof(tempdir), sb.str());

      if (!checkDirExists(tempdir))
      {
        if (recursiveCreateDirectory(tempdir))
          break;
      }
    }

    try
    {
      Owned<IEnvDeploymentEngine> m_configGenMgr;
      CConfigEngCallback callback(verbose, true);
      m_configGenMgr.setown(createConfigGenMgr(*m_pConstEnvironment, callback, NULL, in_dirname?in_dirname:"", tempdir, NULL, NULL, NULL));
      m_configGenMgr->deploy(DEFLAGS_CONFIGFILES, DEBACKUP_NONE, false, false);
      deleteRecursive(tempdir);
    }
    catch(IException* e)
    {
      deleteRecursive(tempdir);
      throw e;
    }
  }
  else if (!listComps && !listallComps && !listdirs && !listcommondirs && !listMachines)
  {
    Owned<IEnvDeploymentEngine> m_configGenMgr;
    m_configGenMgr.setown(createConfigGenMgr(*m_pConstEnvironment, callback, NULL, in_dirname?in_dirname:"", out_dirname?out_dirname:"", compName, compType, ipAddr));
    m_configGenMgr->deploy(DEFLAGS_CONFIGFILES, DEBACKUP_NONE, false, false);
  }
  else if (listdirs)
  {
    StringBuffer out;
    xPath.clear().appendf("Software/%s", XML_TAG_DROPZONE);
    Owned<IPropertyTreeIterator> dropZonesInsts = pEnv->getElements(xPath.str());
    ForEach(*dropZonesInsts)
    {
      IPropertyTree* pDropZone = &dropZonesInsts->query();
      StringBuffer computerName(pDropZone->queryProp(XML_ATTR_COMPUTER));
      xPath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computerName.str());
      IPropertyTree* pComputer = pEnv->queryPropTree(xPath.str());
      if (pComputer)
      {
        const char* netAddr = pComputer->queryProp("@netAddress");
        if (matchDeployAddress(ipAddr, netAddr))
          out.appendf("%s\n", pDropZone->queryProp(XML_ATTR_DIRECTORY)); 
      }
    }

    fprintf(stdout, "%s", out.str());
  }
  else if (listcommondirs)
  {
    StringBuffer out;
    StringBuffer name;
    xPath.clear().appendf("Software/Directories/@name");
    name.append(pEnv->queryProp(xPath.str()));

    xPath.clear().appendf("Software/Directories/Category");
    Owned<IPropertyTreeIterator> dirInsts = pEnv->getElements(xPath.str());
    ForEach(*dirInsts)
    {
      IPropertyTree* pDir = &dirInsts->query();
      StringBuffer dirName(pDir->queryProp("@dir"));
      int len = strrchr(dirName.str(), '/') - dirName.str();
      dirName.setLength(len);

      if (strstr(dirName.str(), "/[INST]") || strstr(dirName.str(), "/[COMPONENT]"))
        continue;

      dirName.replaceString("[NAME]", name.str());
      out.appendf("%s=%s\n", pDir->queryProp(XML_ATTR_NAME), dirName.str()); 
    }

    fprintf(stdout, "%s", out.str());
  }
  else if (listMachines)
  {
    StringBuffer out;
    Owned<IPropertyTreeIterator> computers = pEnv->getElements("Hardware/Computer");
    ForEach(*computers)
    {
      IPropertyTree* pComputer = &computers->query();
      const char *netAddress = pComputer->queryProp("@netAddress");
      StringBuffer xpath;
      const char* name = pComputer->queryProp(XML_ATTR_NAME);
      bool isHPCCNode = false, isSqlOrLdap = false;

      xpath.clear().appendf(XML_TAG_SOFTWARE"/*[//"XML_ATTR_COMPUTER"='%s']", name);
      Owned<IPropertyTreeIterator> it = pEnv->getElements(xpath.str());

      ForEach(*it)
      {
        IPropertyTree* pComponent = &it->query();

        if (!strcmp(pComponent->queryName(), "MySQLProcess") ||
            !strcmp(pComponent->queryName(), "LDAPServerProcess"))
          isSqlOrLdap = true;
        else
        {
          isHPCCNode = true;
          break;
        }
      }

      if (!isHPCCNode && isSqlOrLdap)
        continue;

      out.appendf("%s,", netAddress  ? netAddress : "");
      const char *computerType = pComputer->queryProp("@computerType");

      if (computerType)
      {
        xpath.clear().appendf("Hardware/ComputerType[@name='%s']", computerType);
        IPropertyTree *pType = pEnv->queryPropTree(xpath.str());
        out.appendf("%s", pType->queryProp("@opSys"));
      }
      out.newline();
    }

    fprintf(stdout, "%s", out.str());
  }
  else
  {
    StringBuffer out;
    Owned<IPropertyTree> pSelectedComponents = getInstances(&m_pConstEnvironment->getPTree(), compName, compType, ipAddr, true);
    Owned<IPropertyTreeIterator> it = pSelectedComponents->getElements("*");

    ForEach(*it)
    {
      IPropertyTree* pComponent = &it->query();

      if (listComps)
      {
        if (!strcmp(pComponent->queryProp("@buildSet"), "roxie") || !strcmp(pComponent->queryProp("@buildSet"), "thor"))
        {
          StringBuffer sbChildren;
          bool isMaster = false;
          Owned<IPropertyTreeIterator> itInst = pComponent->getElements("*");
          ForEach(*itInst)
          {
            IPropertyTree* pInst = &itInst->query();
            String instName(pInst->queryName());
            if (!strcmp(instName.toCharArray(), "ThorMasterProcess") || instName.startsWith("RoxieServerProcess"))
            {
              isMaster = true;
              out.appendf("%s=%s;%s%c%s;%s\n", pComponent->queryProp("@name"), pComponent->queryProp("@buildSet"), out_dirname, PATHSEPCHAR, pComponent->queryProp("@name"),"master");
            }
            else if (!strcmp(instName.toCharArray(), "ThorSlaveProcess") || !strcmp(instName.toCharArray(), "RoxieSlaveProcess"))
              sbChildren.appendf("%s=%s;%s%c%s;%s\n", pComponent->queryProp("@name"), pComponent->queryProp("@buildSet"), out_dirname, PATHSEPCHAR, pComponent->queryProp("@name"),"slave");
          }

          if (!isMaster)
            out.append(sbChildren);
        }
        else
          out.appendf("%s=%s;%s%c%s\n", pComponent->queryProp("@name"), pComponent->queryProp("@buildSet"), out_dirname, PATHSEPCHAR, pComponent->queryProp("@name"));
      }
      else if (listallComps)
      {
        StringBuffer netAddr;
        StringBuffer port;
        StringBuffer processName(pComponent->queryName());
        bool multiInstances = false;

        if(!strcmp(processName.str(), "ThorCluster") || !strcmp(processName.str(), "RoxieCluster"))
        {
          processName.clear();
          multiInstances = true;
        }

        if (pComponent->numChildren())
        {
          Owned<IPropertyTreeIterator> itComp = pComponent->getElements("*");
        
          ForEach(*itComp)
          {
            IPropertyTree* pInst = &itComp->query();
            netAddr.clear().append(pInst->queryProp("@netAddress"));
            port.clear().append(pInst->queryProp("@port"));
            
            if (multiInstances)
              processName.clear().append(pInst->queryName());

            out.appendf("%s,%s,%s,%s,%s%c%s,%s\n", processName.str(), 
              pComponent->queryProp("@name"), netAddr.str(), port.str(), 
              STANDARD_OUTDIR, PATHSEPCHAR, pComponent->queryProp("@name"), pComponent->queryProp("@logDir"));
          }
        }
        else 
        {
          netAddr.clear().append(pComponent->queryProp("@netAddress"));
          port.clear().append(pComponent->queryProp("@port"));
          out.appendf("%s,%s,%s,%s,%s%c%s,%s\n", pComponent->queryName(), 
            pComponent->queryProp("@name"), netAddr.str(), port.str(), 
              STANDARD_OUTDIR, PATHSEPCHAR, pComponent->queryProp("@name"), pComponent->queryProp("@logDir"));
        }
      }
    }

    fprintf(stdout, "%s", out.str());
  }

  return 0;
}

int main(int argc, char** argv)
{
  InitModuleObjects();

  Owned<IProperties> globals = createProperties(true);
  const char* in_filename = NULL;
  const char* in_cfgname = NULL;
  const char* out_dirname = STANDARD_OUTDIR;
  const char* in_dirname = STANDARD_INDIR;
  const char* out_filename = NULL;
  const char* compName = NULL;
  const char* compType = NULL;
  StringBuffer ipAddr = NULL;
  bool generateOutput = true;
  bool listComps = false;
  bool verbose = false;
  bool listallComps = false;
  bool listdirs = false;
  bool listcommondirs = false;
  bool listMachines = false;
  bool validateOnly = false;

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
      in_cfgname = argv[i++];
    }
    else if (stricmp(argv[i], "-c") == 0)
    {
      i++;
      compName = argv[i++];
    }
    else if (stricmp(argv[i], "-t") == 0)
    {
      i++;
      compType = argv[i++];
    }
    else if (stricmp(argv[i], "-ip") == 0)
    {
      i++;
      ipAddr.append(argv[i++]);
      if (strcmp(ipAddr, ".")!=0)
      {
        IpAddress ip(ipAddr.str());
        ipAddr.clear();
        if (ip.isLoopBack())                // assume they meant any local ip... not sure this is a good idea
          ipAddr.append('.');
        else
          ip.getIpText(ipAddr);
      }
    }
    else if(stricmp(argv[i], "-od") == 0)
    {
      i++;
      out_dirname = argv[i++];
    }
    else if(stricmp(argv[i], "-id") == 0)
    {
      i++;
      in_dirname = argv[i++];
    }
    else if (stricmp(argv[i], "-list") == 0)
    {
      i++;
      listComps = true;
    }
    else if (stricmp(argv[i], "-listall") == 0)
    {
      i++;
      listallComps = true;
    }
    else if (stricmp(argv[i], "-listdirs") == 0)
    {
      i++;
      listdirs = true;
    }
    else if (stricmp(argv[i], "-listcommondirs") == 0)
    {
      i++;
      listcommondirs = true;
    }
    else if (stricmp(argv[i], "-machines") == 0)
    {
      i++;
      listMachines = true;
    }
    else if (stricmp(argv[i], "-validateonly") == 0)
    {
      i++;
      validateOnly = true;
    }
    else if (stricmp(argv[i], "-v") == 0)
    {
      i++;
      verbose = true;
    }
    else
    {
      fprintf(stderr, "Error: unknown command line parameter: %s\n", argv[i]);
      usage();
      releaseAtoms();
      return 1;
    }
  }

  if (!in_cfgname)
  {
    fprintf(stderr, "Error: Environment xml file is required. Please specify.\n");
    usage();
    releaseAtoms();
    return 1;
  }

  if (ipAddr.length() == 0  && !listallComps && !validateOnly && !listcommondirs && !listMachines)
    ipAddr.clear().append("."); // Meaning match any local address

  try
  {
    processRequest(in_cfgname, out_dirname, in_dirname, compName, 
      compType,in_filename, out_filename, generateOutput, ipAddr.length() ? ipAddr.str(): NULL,
      listComps, verbose, listallComps, listdirs, listcommondirs, listMachines, validateOnly);
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

