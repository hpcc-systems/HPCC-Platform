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
#include "configengcallback.hpp"
#include "deploy.hpp"
#include "build-config.h"
#include "jutil.hpp"
#include "jhash.ipp"
#include "portlist.h"

#define STANDARD_INDIR COMPONENTFILES_DIR"/configxml"
#define STANDARD_OUTDIR RUNTIME_DIR

void usage()
{
  const char* version = "1.2";
  printf("HPCC Systems® configuration generator. version %s. Usage:\n", version);
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
  puts("          For Linux, '" COMPONENTFILES_DIR "/configxml/'");
  puts("   -od <output directory>: The output directory for the generated files.");
  puts("          If not specified, the following defaults are used. ");
  puts("          For win32, '.'");
  puts("          For Linux, '" CONFIG_DIR "'");
  puts("   -ldapconfig : Generates a .ldaprc file and puts it in the specified");
  puts("          output directory. If output directory is not specified,");
  puts("          default output directory is used as mentioned in -od option");
  puts("          if a LDAPServer is not defined in the environment, the .ldaprc ");
  puts("          file is not generated. If an -ip is not provided, the first");
  puts("          instance of the first LDAPserver is used to generate the ");
  puts("          .ldaprc file");
  puts("   -list: Lists out the components for a specific ip in the format");
  puts("          componentType=componentName;config file directory. Does not ");
  puts("          generate any output files. If masters and slaves exist for ");
  puts("          a component like Roxie or thor, then only the master entry ");
  puts("          is returned. ");
  puts("   -listall: Lists out all the components specified in the environment");
  puts("          that have an instance defined. Does not require an ip. Does ");
  puts("          not generate any output files. Output is written to stdout ");
  puts("          in the csv format as follows");
  puts("          ProcessType,componentName,instanceip,instanceport,runtimedir,logdir");
  puts("          Missing fields will be empty.");
  puts("   -listall2: Same as -listall but includes ThorSlaveProcesses and ThorSpareProcess.");
  puts("   -listespservices: List all esp and their bound services and ports. Does ");
  puts("          not require an ip. Does not generate any output files. Output is written to stdout ");
  puts("          in the csv format as follows");
  puts("          componentType,componentName,serviceType,serviceBindingName,instanceIP,instanceport,protocol");
  puts("          Missing fields will be empty.");
  puts("   -listdirs: Lists out any directories that need to be created during ");
  puts("          init time. Currently, directories for any drop zones ");
  puts("          with the same ip as the -ip option are returned. Format is ");
  puts("          one directory per line.");
  puts("   -listdropzones: Lists out all the dropzones defined in the environment ");
  puts("          Does not require an ip. Does not generate any output files.");
  puts("          Output is written to stdout. Format is as follows,");
  puts("          one entry per line");
  puts("          dropzone node ip,dropzone directory");
  puts("   -listcommondirs: Lists out all directories that are listed under ");
  puts("          Software/Directories section in the following format. ");
  puts("          <CategoryName>=<DirectoryValue>");
  puts("          Each directory will be listed on a new line.");
  puts("   -listldaps: Lists out all LDAPServer instances defined in the ");
  puts("          environment in the following format. If the same component");
  puts("          has more than one instance, it will be listed as two separate.");
  puts("          entries in the output");
  puts("          componentName,instanceip");
  puts("   -listldapservers: Lists out all LDAP Server components and associated tags");
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

void copyDirectoryRecursive(const char *source, const char *target)
{
  bool first = true;
  Owned<IDirectoryIterator> dir = createDirectoryIterator(source, "*");

  ForEach (*dir)
  {
    IFile &sourceFile = dir->query();

    if (sourceFile.isFile())
    {
      StringBuffer targetname(target);
      targetname.append(PATHSEPCHAR);
      dir->getName(targetname);
      OwnedIFile destFile = createIFile(targetname.str());

      if (first)
      {
        if (!recursiveCreateDirectory(target))
          throw MakeStringException(-1,"Cannot create directory %s",target);

        first = false;
      }

      copyFile(destFile, &sourceFile);
    }
    else if (sourceFile.isDirectory())
    {
      StringBuffer newSource(source);
      StringBuffer newTarget(target);
      newSource.append(PATHSEPCHAR);
      newTarget.append(PATHSEPCHAR);
      dir->getName(newSource);
      dir->getName(newTarget);
      copyDirectoryRecursive(newSource.str(), newTarget.str());
    }
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
                   bool listComps, bool verbose, bool listallComps, bool listallCompsAllThors, bool listESPServices, bool listdirs,
                   bool listdropzones, bool listcommondirs, bool listMachines, bool validateOnly,
                   bool listldaps, bool ldapconfig, bool ldapservers)
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
  else if (ldapconfig)
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

    StringBuffer out;
    xPath.clear().append(XML_TAG_SOFTWARE "/" XML_TAG_LDAPSERVERPROCESS);
    Owned<IPropertyTreeIterator> ldaps = pEnv->getElements(xPath.str());
    Owned<IPropertyTree> pSelComps(createPTree("SelectedComponents"));
    bool flag = false;

    ForEach(*ldaps)
    {
      IPropertyTree* ldap = &ldaps->query();
      IPropertyTree* inst;
      int count = 1;
      xPath.clear().appendf(XML_TAG_INSTANCE"[%d]", count);

      while ((inst = ldap->queryPropTree(xPath.str())) != NULL)
      {
        if (ipAddr && *ipAddr && strcmp(ipAddr, inst->queryProp(XML_ATTR_NETADDRESS)))
        {
          ldap->removeTree(inst);
          continue;
        }

        if (!flag)
        {
          inst->addProp(XML_ATTR_DIRECTORY, ".");
          sb.clear().append(tempdir).append(PATHSEPCHAR).append(ldap->queryProp(XML_ATTR_NAME));
          xPath.clear().appendf(XML_TAG_INSTANCE"[%d]", ++count);
          flag = true;
        }
        else
        {
          ldap->removeTree(inst);
        }
      }

      if (flag)
      {
        pSelComps->addPropTree(XML_TAG_LDAPSERVERPROCESS, createPTreeFromIPT(ldap));
        break;
      }
    }

    if (flag)
    {
      try
      {
        toXML(pEnv, envXML.clear());
        m_pEnvironment.setown(factory->loadLocalEnvironment(envXML));
        m_pConstEnvironment.set(m_pEnvironment);
        Owned<IEnvDeploymentEngine> m_configGenMgr;
        m_configGenMgr.setown(createConfigGenMgr(*m_pConstEnvironment, callback, pSelComps, in_dirname?in_dirname:"", tempdir, compName, compType, ipAddr));
        m_configGenMgr->deploy(DEFLAGS_CONFIGFILES, DEBACKUP_NONE, false, false);
        copyDirectoryRecursive(sb.str(), out_dirname);
        deleteRecursive(tempdir);
      }
      catch (IException* e)
      {
        deleteRecursive(tempdir);
        throw e;
      }
    }
  }
  else if (ldapservers)
  {
    //first, build a map of all ldapServer/IP
    StringBuffer sb1,sb2;
    typedef MapStringTo<StringBuffer> strMap;
    strMap ldapServers;
    {
      xPath.appendf("Software/%s/", XML_TAG_LDAPSERVERPROCESS);
      Owned<IPropertyTreeIterator> ldaps = pEnv->getElements(xPath.str());
      ForEach(*ldaps)
      {
        IPropertyTree* ldap = &ldaps->query();
        Owned<IPropertyTreeIterator> insts = ldap->getElements(XML_TAG_INSTANCE);
        ForEach(*insts)
        {
          IPropertyTree* inst = &insts->query();
          StringBuffer computerName(inst->queryProp(XML_ATTR_COMPUTER));
          xPath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computerName.str());
          IPropertyTree* pComputer = pEnv->queryPropTree(xPath.str());
          if (pComputer)
          {
            if (NULL == ldapServers.getValue(ldap->queryProp(XML_ATTR_NAME)))//only add once
              ldapServers.setValue(ldap->queryProp(XML_ATTR_NAME), pComputer->queryProp("@netAddress"));
          }
        }
      }
    }

    //Lookup and output all LDAPServer components
    StringBuffer out;
    xPath.clear().append(XML_TAG_SOFTWARE "/" XML_TAG_LDAPSERVERPROCESS);
    Owned<IPropertyTreeIterator> ldaps = pEnv->getElements(xPath.str());

    ForEach(*ldaps)
    {
      IPropertyTree* ldap = &ldaps->query();
      out.appendf("%s\n",ldap->queryName());
      Owned<IAttributeIterator> attrs = ldap->getAttributes();
      ForEach(*attrs)
      {
        out.appendf("%s,%s\n", attrs->queryName(), attrs->queryValue());
        //If this is ldap server name, lookup and add its IP address
        if (0==strcmp(attrs->queryName(), "@name"))
        {
          StringBuffer * ldapIP = ldapServers.getValue(attrs->queryValue());
          if (ldapIP)
          {
            out.appendf("@ldapAddress,%s\n",ldapIP->str());
            ldapServers.remove(attrs->queryValue());//ensure this server only listed once
          }
          else
          {
            out.clear();
            break;
          }
        }
      }
      if (out.length())
      {
        fprintf(stdout, "%s", out.str());
        out.clear();
      }
    }
  }
  else if (!listComps && !listallComps && !listdirs && !listdropzones && !listcommondirs && !listMachines
           && !listldaps && !listESPServices)
  {
    Owned<IEnvDeploymentEngine> m_configGenMgr;
    m_configGenMgr.setown(createConfigGenMgr(*m_pConstEnvironment, callback, NULL, in_dirname?in_dirname:"", out_dirname?out_dirname:"", compName, compType, ipAddr));
    m_configGenMgr->deploy(DEFLAGS_CONFIGFILES, DEBACKUP_NONE, false, false);
  }
  else if (listldaps)
  {
    StringBuffer out;
    xPath.appendf("Software/%s/", XML_TAG_LDAPSERVERPROCESS);
    Owned<IPropertyTreeIterator> ldaps = pEnv->getElements(xPath.str());

    ForEach(*ldaps)
    {
      IPropertyTree* ldap = &ldaps->query();
      Owned<IPropertyTreeIterator> insts = ldap->getElements(XML_TAG_INSTANCE);

      ForEach(*insts)
      {
        IPropertyTree* inst = &insts->query();
        StringBuffer computerName(inst->queryProp(XML_ATTR_COMPUTER));
        xPath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computerName.str());
        IPropertyTree* pComputer = pEnv->queryPropTree(xPath.str());

        if (pComputer)
        {
          const char* netAddr = pComputer->queryProp("@netAddress");
          out.appendf("%s,%s\n", ldap->queryProp(XML_ATTR_NAME), netAddr);
        }
      }
    }

    fprintf(stdout, "%s", out.str());
  }
  else if (listdirs || listdropzones)
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

        if (listdropzones)
          out.appendf("%s,%s\n", netAddr, pDropZone->queryProp(XML_ATTR_DIRECTORY));
        else if (matchDeployAddress(ipAddr, netAddr))
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
  else if (listESPServices == true)
  {
      StringBuffer out;
      Owned<IPropertyTreeIterator> espProcesses = pEnv->getElements(XML_TAG_SOFTWARE "/" XML_TAG_ESPPROCESS);

      ForEach(*espProcesses)
      {
          StringBuffer strNetAddr;
          StringBuffer strPort;

          IPropertyTree *pComponent = &espProcesses->query();

          StringBuffer processName(pComponent->queryName()); // component type

          if(strcmp(processName.str(), XML_TAG_ESPPROCESS) != 0)
          {
              continue;
          }

          StringBuffer strEspName(pComponent->queryProp(XML_ATTR_NAME)); //esp name

          Owned<IPropertyTreeIterator> itInstances = pComponent->getElements(XML_TAG_INSTANCE);

          ForEach(*itInstances)
          {
              StringBuffer strInstanceName;
              IPropertyTree* pInst = &itInstances->query();

              strNetAddr.clear().append(pInst->queryProp(XML_ATTR_NETADDRESS));
              strInstanceName.clear().append(pInst->queryProp(XML_ATTR_NAME));

              Owned<IPropertyTreeIterator> itBinding = pComponent->getElements(XML_TAG_ESPBINDING);

              ForEach(*itBinding)
              {
                  IPropertyTree* pBinding = &itBinding->query();
                  StringBuffer strServiceName(pBinding->queryProp(XML_ATTR_SERVICE));
                  StringBuffer strBindingName(pBinding->queryProp(XML_ATTR_NAME));
                  StringBuffer strProtocol(pBinding->queryProp(XML_ATTR_PROTOCOL));

                  strPort.clear().append(pBinding->queryProp(XML_ATTR_PORT));

                  out.appendf("%s,%s,%s,%s,%s,%s,%s\n", processName.str(), strEspName.str(), strServiceName.str(),strBindingName.str(), strNetAddr.str(), strPort.str(),strProtocol.str());
              }
          }
      }
      fprintf(stdout, "%s", out.str());
  }
  else if (listMachines)
  {
    MapStringTo<bool> mapOfMachineIPs;
    StringBuffer out;
    Owned<IPropertyTreeIterator> computers = pEnv->getElements("Hardware/Computer");
    ForEach(*computers)
    {
      IPropertyTree* pComputer = &computers->query();
      const char *netAddress = pComputer->queryProp("@netAddress");

      if (mapOfMachineIPs.getValue(netAddress) != NULL)
      {
          continue;
      }
      else
      {
          mapOfMachineIPs.setValue(netAddress, true);
      }

      StringBuffer xpath;
      const char* name = pComputer->queryProp(XML_ATTR_NAME);
      bool isNonHPCCNode = true;

      xpath.clear().appendf(XML_TAG_SOFTWARE "/*[//" XML_ATTR_COMPUTER "='%s']", name);
      Owned<IPropertyTreeIterator> it = pEnv->getElements(xpath.str());

      ForEach(*it)
      {
        IPropertyTree* pComponent = &it->query();

        if (!strcmp(pComponent->queryName(), "LDAPServerProcess") ||
            !strcmp(pComponent->queryName(), "MySQLProcess"))
            // because if either blacklisted components are also on a machine that
            // has a valid component, we still want to show that machine.
            continue;
        else
            // iterator is valid, and not blacklisted
            isNonHPCCNode = false;
            break;
      }

      if (isNonHPCCNode)
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
          Owned<IPropertyTreeIterator> itInst = pComponent->getElements("*");
          ForEach(*itInst)
          {
            IPropertyTree* pInst = &itInst->query();
            String instName(pInst->queryName());
            if (!strcmp(instName.str(), "ThorMasterProcess") || instName.startsWith("RoxieServerProcess"))
            {
              out.appendf("%s=%s;%s%c%s;%s\n", pComponent->queryProp("@name"), pComponent->queryProp("@buildSet"), out_dirname, PATHSEPCHAR, pComponent->queryProp("@name"),"master");
            }
          }
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

            if (listallCompsAllThors == false && (!strcmp(pInst->queryName(), "ThorSlaveProcess") || !strcmp(pInst->queryName(), "ThorSpareProcess")))
              continue;

            netAddr.clear().append(pInst->queryProp("@netAddress"));
            port.clear();

            if (strcmp(pInst->queryName(), XML_TAG_THORMASTERPROCESS) == 0)
            {
                if (pInst->queryProp(XML_ATTR_MASTERPORT) == NULL || (pInst->queryProp(XML_ATTR_MASTERPORT))[0] == 0)
                    port.append(THOR_BASE_PORT);
                else
                    port.append(pInst->queryProp(XML_ATTR_MASTERPORT));
            }
	    else
		port.append(pInst->queryProp("@port"));
            
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
  StringBuffer ipAddr;
  bool generateOutput = true;
  bool listComps = false;
  bool verbose = false;
  bool listallComps = false;
  bool listallCompsAllThors = false;
  bool listdirs = false;
  bool listdropzones = false;
  bool listcommondirs = false;
  bool listMachines = false;
  bool validateOnly = false;
  bool ldapconfig = false;
  bool listldaps = false;
  bool listespservices = false;
  bool ldapservers = false;

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
    else if (stricmp(argv[i], "-ldapconfig") == 0)
    {
      i++;
      ldapconfig = true;
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
    else if (stricmp(argv[i], "-listall2") == 0)
    {
      i++;
      listallComps = true;
      listallCompsAllThors = true;
    }
    else if (stricmp(argv[i], "-listespservices") == 0)
    {
        i++;
        listespservices = true;
    }
    else if (stricmp(argv[i], "-listdirs") == 0)
    {
      i++;
      listdirs = true;
    }
    else if (stricmp(argv[i], "-listdropzones") == 0)
    {
      i++;
      listdropzones = true;
    }
    else if (stricmp(argv[i], "-listcommondirs") == 0)
    {
      i++;
      listcommondirs = true;
    }
    else if (stricmp(argv[i], "-listldaps") == 0)
    {
      i++;
      listldaps = true;
    }
    else if (stricmp(argv[i], "-listldapservers") == 0)
    {
      i++;
      ldapservers = true;
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

  if (ipAddr.length() == 0  && !listallComps && !validateOnly && !listcommondirs && !listMachines && !listldaps && !ldapconfig)
    ipAddr.clear().append("."); // Meaning match any local address

  try
  {
    processRequest(in_cfgname, out_dirname, in_dirname, compName, 
      compType,in_filename, out_filename, generateOutput, ipAddr.length() ? ipAddr.str(): NULL,
      listComps, verbose, listallComps, listallCompsAllThors, listespservices, listdirs, listdropzones, listcommondirs, listMachines,
      validateOnly, listldaps, ldapconfig, ldapservers);
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

