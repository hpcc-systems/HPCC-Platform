/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jptree.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "environment.hpp"
#include "xslprocessor.hpp"
#include "configgenengine.hpp"
#include "XMLTags.h"
#include "confighelper.hpp"

#ifdef _WINDOWS 
#define CONFIGGEN_COMP_LIST "cgencomplist_win.xml"
#else
#define CONFIGGEN_COMP_LIST "cgencomplist_linux.xml"
#endif

//---------------------------------------------------------------------------
// CEspDeploymentEngine
//---------------------------------------------------------------------------
CConfigGenEngine::CConfigGenEngine(IEnvDeploymentEngine& envDepEngine, 
                     IDeploymentCallback& callback, IPropertyTree& process, 
                     const char* inputDir, const char* outputDir, 
                     const char* instanceType, bool createIni)
 : CDeploymentEngine(envDepEngine, callback, process, instanceType, createIni),m_inDir(inputDir), m_outDir(outputDir)
{
  m_useSSHIfDefined = false;
}


//---------------------------------------------------------------------------
//  determineInstallFiles
//---------------------------------------------------------------------------
int CConfigGenEngine::determineInstallFiles(IPropertyTree& processNode, CInstallFiles& installFiles) const
{
    try
    {
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, 
            "Determining files to install for %s", processNode.queryProp("@name"));

        StringBuffer compListPath(CONFIGGEN_COMP_LIST);
        if (m_inDir.length())
            compListPath.clear().append(m_inDir).append(PATHSEPCHAR).append(CONFIGGEN_COMP_LIST);

        Owned<IPropertyTree> deployNode = createPTreeFromXMLFile(compListPath.str(), ipt_caseInsensitive);

        CConfigHelper *pConfigHelper = CConfigHelper::getInstance(NULL, NULL, m_pCallback.get());

        compListPath.clear().set(m_inDir).append(PATHSEPCHAR);
        pConfigHelper->addPluginsToConfigGenCompList(deployNode.get(), compListPath.str());

        StringBuffer srcFilePath;
        srcFilePath.ensureCapacity(_MAX_PATH);
        const bool bFindStartable = &m_process == &processNode && m_startable == unknown;
        const bool bFindStoppable = &m_process == &processNode && m_stoppable == unknown;

        StringBuffer xpath;
        xpath.appendf("Component[@name=\"%s\"]",processNode.queryProp("@buildSet"));
        IPropertyTree* pComponent = deployNode->queryPropTree(xpath.str());

        if (!pComponent)
        {
            m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, 
                "Cannot find files to install for %s", processNode.queryProp("@buildSet"));
            return 0;
        }

        Owned<IPropertyTreeIterator> iter = pComponent->getElements("File");
        ForEach(*iter)
        {
            IPropertyTree* pFile = &iter->query();
            const char* name = pFile->queryProp("@name");

            if (!stricmp(name, "deploy_map.xml"))
                continue;

            if (bFindStartable && !strnicmp(name, "startup", sizeof("startup")-1))
                m_startable = yes;

            if (bFindStoppable && !strnicmp(name, "stop", sizeof("stop")-1))
                m_stoppable = yes;

            const char* method  = pFile->queryProp("@method");
            if (method && !stricmp(method, "schema"))
                continue;

            //if we are not deploying build files and method is copy then ignore this file
            if (!(m_deployFlags & DEFLAGS_BUILDFILES) && (!method || !stricmp(method, "copy")))
                continue;

            const char* srcPath = pFile->queryProp("@srcPath");
            const char* destPath= pFile->queryProp("@destPath");
            const char* destName= pFile->queryProp("@destName");
            bool bCacheable     = pFile->getPropBool("@cache", false);

            // Get source filespec
            if (srcPath && !strcmp(srcPath, "atmark_temp"))
            {
                char tempfile[_MAX_PATH];
                getTempPath(tempfile, sizeof(tempfile), m_name);
                srcFilePath.clear().append(tempfile).append(name);
            }
            else
            {
                srcFilePath.clear().append(m_inDir);

        //adjust source paths
                if (srcPath && 0!=strcmp(srcPath, "."))
                {
                    if (!strncmp(srcPath, "..", 2) && (*(srcPath+2)=='/' || *(srcPath+2)=='\\'))
                    {
                        StringBuffer reldir(srcPath);
                        reldir.replace('/', '\\');

                        while (!strncmp(reldir.str(), "..\\", 3))
                        {
                            srcFilePath.setLength( srcFilePath.length() - 1 ); //remove last char PATHSEPCHAR
                            const char* tail = pathTail(srcFilePath.str());
                            srcFilePath.setLength( tail - srcFilePath.str() );
                            reldir.remove(0, 3);
                        }
                        srcFilePath.append(reldir).append(PATHSEPCHAR);
                    }
                    else
                        srcFilePath.append(srcPath).append(PATHSEPCHAR);
                }
                srcFilePath.append(name);
            }

            std::string sDestName;
            if (method && (!stricmp(method, "esp_service_module") || !stricmp(method, "esp_plugin")))
            {
                //if this is xsl transformation and we are not generating config files then ignore
                //
                if (!(m_deployFlags & DEFLAGS_CONFIGFILES) && !stricmp(method, "esp_service_module"))
                    continue;

                //if this file is an esp service module, encode name of service in the dest file name
                //so the esp deployment can figure out which service this file belongs to
                //
                const char* serviceName = processNode.queryProp("@name");

                //if destination name is specified then use it otherwise use <service-name>[index of module].xml
                sDestName = serviceName;
                if (destName)
                {
                    sDestName += '_';
                    sDestName += destName;
                }
                else
                {
                    int espServiceModules = m_envDepEngine.incrementEspModuleCount();
                    if (espServiceModules > 1)
                    {
                        char achNum[16];
                        itoa(espServiceModules, achNum, 10);

                        sDestName += achNum;
                    }
                    sDestName += ".xml";
                }
                //encode name of service herein - this is needed by and removed by CEspDeploymentEngine::processServiceModules()
                sDestName += '+';
                sDestName += processNode.queryProp("@name");//encode the name of service
            }
            else if (method && (!stricmp(method, "xsl") || !stricmp(method, "xslt")) && !(m_deployFlags & DEFLAGS_CONFIGFILES))
                continue;//ignore xsl transformations if we are not generating config files
            else
            {
                if (!method || !*method)
                    method = "copy";

                // Get destination filespec
                if (destName && *destName)
                {
                    //we now support attribute names within the destination file names like delimted by @ and + (optional)
                    //for e.g. segment_@attrib1+_file_@attrib2 would produce segment_attribval1_file_attrib2value
                    //+ not necessary if the attribute name ends with the word, for e.g. file_@attrib1
                    //for instnace, suite_@eclServer+.bat would expand to suite_myeclserver.bat
                    //if this process has an @eclServer with value "myeclserver"
                    //
                    if (strchr(destName, '@') || strchr(destName, '+'))
                    {
                        char* pszParts = strdup(destName);

                        char *saveptr;
                        const char* pszPart = strtok_r(pszParts, "+", &saveptr);
                        while (pszPart)
                        {
                            const char* p = pszPart;
                            if (*p)
                            {
                                if (strchr(p, '@'))//xpath for an attribute?
                                {
                                    // find name of attribute and replace it with its value
                                    const char* value = m_process.queryProp( p );
                                    if (value)
                                        sDestName.append(value);
                                }
                                else
                                    sDestName.append(p); //no attribute so copy verbatim
                            }
                            pszPart = strtok_r(NULL, "+", &saveptr);
                        }
                        free(pszParts);
                    }
                    else
                        sDestName = destName;


                    if (sDestName.empty())
                        throw MakeStringException(-1, "The destination file name '%s' for source file '%s' "
                        "translates to an empty string!", destName, name);
                }
            }

            StringBuffer destFilePath;
            destFilePath.ensureCapacity(_MAX_PATH);

            bool bTempFile = (destPath && !stricmp(destPath, "atmark_temp")) ||
                !strnicmp(name, "atmark_temp", 5); //@name starts with atmark_temp or @tmp
            if (bTempFile)
            {
                if (sDestName.empty())//dest name not specified
                {
                    if (!strcmp(method, "copy"))
                        sDestName = name;
                    else
                    {
                        StringBuffer dir;
                        const char* pszFileName = splitDirTail(name, dir);
                        const char* pExt = findFileExtension(pszFileName);

                        if (pExt)
                            sDestName.append(pszFileName, pExt-pszFileName);
                        else
                            sDestName.append(pszFileName);

                        char index[16];
                        itoa(m_envDepEngine.incrementTempFileCount(), index, 10);
                        sDestName.append(index);

                        if (pExt)
                            sDestName.append(pExt);
                    }
                }
                destFilePath.append("atmark_temp" PATHSEPSTR);
            }
            else
            {
                if (destPath && *destPath)
                {
                    destFilePath.append(destPath);
                    if (destPath[strlen(destPath)-1] != PATHSEPCHAR)
                        destFilePath.append(PATHSEPCHAR);
                }

                if (sDestName.empty())
                    sDestName = name;
            }

            if (!bTempFile)
                destFilePath.append(processNode.queryProp("@name")).append(PATHSEPCHAR);

            destFilePath.append(sDestName.c_str());

            //For oss, plugins to be handled globally, per Richard.
            //esp plugins also end with plugins.xml but they should be handled above.
            String destFilePathStr(destFilePath);
            String* tmpstr = destFilePathStr.toLowerCase();
            if (tmpstr->indexOf("plugins.xml") > 0)
            {
                delete tmpstr;
                createFakePlugins(destFilePath);
                continue;
            }

            delete tmpstr;

            //find all occurrences of this destination file in the map and resove any conflicts
            //like size mismatch etc.
            bool bAddToFileMap = installFiles.resolveConflicts(processNode, method, srcFilePath.str(), destFilePath.str(),
                m_name, m_curInstance, NULL);
            //resolve conflicts if method is not schema or exec
            if (0 != stricmp(method, "schema") && 0 != stricmp(method, "exec") && 0 != strnicmp(method, "del", 3)) 
            {
            }
            else if (!strnicmp(method, "del", 3))//treat files to be deleted as temp files - to be deleted AFTER we are done!
            {
                bTempFile = true;
                bAddToFileMap = false;
                m_envDepEngine.addTempFile(destFilePath.str());
            }


            if (bAddToFileMap)
            {
                if (bTempFile)
                    m_envDepEngine.addTempFile(destFilePath.str());

                //enable caching for files to be copied unless expressly asked not to do so
                //
                if (!bCacheable && !strcmp(method, "copy"))
                    bCacheable = pFile->getPropBool("@cache", true);

                installFiles.addInstallFile(method, srcFilePath.str(), destFilePath.str(), bCacheable, NULL);
            }
        }
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(0, "Error creating file list for process %s: %s", m_name.get(), msg.str());
    }
    catch (...)
    {
        throw MakeErrnoException("Error creating file list for process %s", m_name.get());
    }

    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, NULL);
    return installFiles.getInstallFileList().size();
}

//---------------------------------------------------------------------------
//  deployInstance
//---------------------------------------------------------------------------
void CConfigGenEngine::deployInstance(IPropertyTree& instanceNode, bool useTempDir)
{
    StringAttr hostDir(m_outDir);
    StringAttr destDir(useTempDir ? getDeployDir(instanceNode).str() : hostDir.get());
    
    const char* pszHostDir = hostDir.get();
    if (pszHostDir && *pszHostDir==PATHSEPCHAR && *(pszHostDir+1)==PATHSEPCHAR)
        connectToHost(instanceNode);
    beforeDeployInstance(instanceNode, destDir);
    copyInstallFiles(instanceNode, destDir);
    afterDeployInstance(instanceNode, destDir);
    
    if (!m_compare && useTempDir)
    {
        checkAbort();
        EnvMachineOS os= m_envDepEngine.lookupMachineOS(instanceNode);
        renameDir(hostDir, NULL, os);
        renameDir(destDir, hostDir, os);
    }
}

void CConfigGenEngine::createFakePlugins(StringBuffer& destFilePath) const
{
    String destFilePathStr(destFilePath);
    String* tmpstr = destFilePathStr.toLowerCase();
    if (!tmpstr->endsWith("plugins.xml"))
    {
        int index = tmpstr->indexOf("plugins.xml");
        destFilePath.remove(index + 11, destFilePath.length() - (index + 11));
    }

    delete tmpstr;

    StringBuffer tmpoutbuf("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Plugins/>");

    if (m_instances.ordinality() > 1 && strcmp(m_process.queryName(), XML_TAG_ESPPROCESS))
        destFilePath.replaceString("atmark_temp"PATHSEPSTR, m_cachePath);
    else
    {
        char tempPath[_MAX_PATH];
        getTempPath(tempPath, sizeof(tempPath), m_name);
        ensurePath(tempPath);
        destFilePath.replaceString("atmark_temp"PATHSEPSTR, tempPath);
    }

    Owned<IFile> pTargetFile = createIFile(destFilePath.str());
    if (pTargetFile->exists() && pTargetFile->isReadOnly())
        pTargetFile->setReadOnly(false);
    Owned<IFileIO> pTargetFileIO = pTargetFile->open(IFOcreate);
    pTargetFileIO->write( 0, tmpoutbuf.length(), tmpoutbuf.str());
    m_envDepEngine.addTempFile(destFilePath.str());
}

//---------------------------------------------------------------------------
//  beforeDeploy
//---------------------------------------------------------------------------
void CConfigGenEngine::beforeDeploy()
{
    m_installFiles.clear();

    char tempPath[_MAX_PATH];
    getTempPath(tempPath, sizeof(tempPath), m_name);
    m_envDepEngine.addTempDirectory( tempPath );

    if (m_instances.ordinality() > 1)
    {
        strcat(tempPath, "Cache");
        char* pszEnd = tempPath + strlen(tempPath);

        Owned<IFile> pFile = createIFile(tempPath);
        int i = 1;

        while (pFile->exists()) { //dir/file exists
            itoa(++i, pszEnd, 10);
            pFile.setown( createIFile(tempPath) );
        }

        strcat(tempPath, PATHSEPSTR);
        m_cachePath.set( tempPath );
    }
    else
        m_cachePath.set( tempPath );

    ensurePath(tempPath);
    determineInstallFiles(m_process, m_installFiles);
    getCallback().installFileListChanged();

    if (m_instances.ordinality() > 1)
    {
        EnvMachineOS os = m_envDepEngine.lookupMachineOS( m_instances.item(0) );
        m_curInstance = "Cache";
        copyInstallFiles("Cache", -1, tempPath, os);
    }
}
