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
#ifndef DEPLOYMENTENGINE_HPP_INCL
#define DEPLOYMENTENGINE_HPP_INCL

#include "jiface.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "deploy.hpp"
#include "environment.hpp"
#include "xslprocessor.hpp"
#include <map>
#include <set>
#include <string>
#include <vector>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif


#define SEISINT_NAMESPACE "http://seisint.com"

interface IPropertyTree;
interface IXslProcessor;
interface IXslTransform;

using namespace std;

struct CInstallFile : public CInterface 
{
   CInstallFile(const char* method, const char* srcPath, const char* destPath, bool bCacheable=false)
        : m_method(method), m_srcPath(srcPath), m_destPath(destPath), m_bCacheable(bCacheable), 
           m_szSrc(0), m_crcSrc(0), m_sizeInitialized(false), m_crcInitialized(false)
    {
    }
    virtual ~CInstallFile()
    {
    }

    IMPLEMENT_IINTERFACE;

   const string& getMethod  () const { return m_method;  }
   const string& getSrcPath () const { return m_srcPath; }
   const string& getDestPath() const { return m_destPath;}
   const string& getParams()   const { return m_params;  }//only supported for dynamically added files
    
    // This function could throw exception
    offset_t getSrcSize()   
    { 
        if (!m_sizeInitialized)
        {
            m_szSrc = filesize(m_srcPath.c_str());
            m_sizeInitialized = true;
        }
        return m_szSrc; 
    }
    
    // This function could throw exception
    unsigned getSrcCRC()
    { 
        if (!m_crcInitialized)
        {
            m_crcSrc = getFileCRC(m_srcPath.c_str());
            m_crcInitialized = true;
        }
        return m_crcSrc; 
    }

    bool  isCacheable()  const { return m_bCacheable;}

   void  setParams(const char* params){ m_params = params;}//only supported for dynamically added files
    void  setMethod(const char* method){ m_method = method;}
   void  setSrcPath(const char* path) { m_srcPath = path; }
    //void  setSrcSize(offset_t sz) { m_szSrc = sz; }
    //void  setSrcCRC(unsigned crc) { m_crcSrc = crc; }
    void  setCacheable(bool val=true) { m_bCacheable = val; }

    bool isDuplicateSrcFile(const char* filename) const
    {
        return m_duplicateSrcFiles.find(filename) != m_duplicateSrcFiles.end();
    }

    void addDuplicateSrcFile(const char* filename)
    {
        m_duplicateSrcFiles.insert(filename);
    }

private:
   string m_method;
   string m_srcPath;
   string m_destPath;
   string m_params;  //only supported for dynamically added files
   offset_t  m_szSrc;
    unsigned m_crcSrc;
    bool m_sizeInitialized;
    bool m_crcInitialized;
    bool   m_bCacheable;

    set<string> m_duplicateSrcFiles;
};

//define a case insensitive comparator class for std::string to be used for 
//implementing a case insensitive multimap below

struct iless_string : std::binary_function<string, string, bool> {
bool operator()(const string& _X, const string& _Y) const
    {return stricmp(_X.c_str(), _Y.c_str()) < 0; }
};

typedef Linked<CInstallFile> LinkedFilePtr;

class CInstallFileMap : public std::multimap<std::string, LinkedFilePtr, iless_string>
{
public:
   CInstallFileMap()
        : m_pDepEngine(NULL)
   {
   }
    virtual ~CInstallFileMap()
    {
    }
   void setDeploymentEngine(IDeploymentEngine& depEngine)
   {
      m_pDepEngine = &depEngine;
   }
   bool resolveConflicts(IPropertyTree& processNode, const char* method, const char* srcPath, const char* destPath,
                         const char* compName, const char* instanceName, const char* params);

private:
   CInstallFileMap(const CInstallFileMap&);
   IDeploymentEngine* m_pDepEngine;
};

typedef vector<Linked<CInstallFile> > CInstallFileList;

struct CInstallFiles 
{
private:
   IDeploymentEngine* m_pDepEngine;
   CInstallFileList   m_list;
   CInstallFileMap    m_map; // multimap of destFilePath -> std::pair<method, sourceFilePath>

   CInstallFiles(const CInstallFiles&);//disallow compiler from generating copy constructor

public:      
   CInstallFiles()
   {
        m_pDepEngine = NULL;
   }
    virtual ~CInstallFiles()
    {
    }

   void setDeploymentEngine(IDeploymentEngine& depEngine)
   {
      m_pDepEngine = &depEngine;
      m_map.setDeploymentEngine(depEngine);
   }
   CInstallFile* addInstallFile(const char* method, const char* srcPath, const char* destPath, bool bCacheable, const char* params)
   {
        LinkedFilePtr pFile = new CInstallFile(method, srcPath, destPath, bCacheable);
        
        if (params)
            pFile->setParams(params);

        m_map.insert(CInstallFileMap::value_type(destPath, pFile));
        m_list.push_back(pFile);
        pFile->Release();
        return pFile;
   }
   void clear()
   {
      m_list.clear();
      m_map.clear();
   }
   CInstallFile* findInstallFile(const char* destPath, string& method, string& srcPath)
   {
      CInstallFileMap::const_iterator it = m_map.find(destPath);
      return it != m_map.end() ? (*it).second : NULL;
   }
   bool resolveConflicts(IPropertyTree& processNode, const char* method, const char* srcPath, const char* destPath,
                         const char* compName, const char* instanceName, const char* params)
   {
      return m_map.resolveConflicts(processNode, method, srcPath, destPath, compName, instanceName, params);
   }
   const CInstallFileList& getInstallFileList() const { return m_list; }
};

//---------------------------------------------------------------------------
//  CDeploymentEngine
//---------------------------------------------------------------------------
class CDeploymentEngine : public CInterface, 
                          implements IDeploymentEngine,
                          implements IExceptionHandler
{
public:
    IMPLEMENT_IINTERFACE;
   CDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
                     IDeploymentCallback& callback, IPropertyTree& process, 
                     const char* instanceType=NULL, bool createIni=false);
    virtual ~CDeploymentEngine();

public: // IDeploymentEngine implementation
   virtual void setXsl(IXslProcessor* processor, IXslTransform* transform);
    virtual void start();
    virtual void stop();
   virtual void check();
   virtual void compare(unsigned flags);
   virtual void deploy(unsigned flags, bool useTempDir);
   virtual void renameDirs();
   virtual void backupDirs();
    virtual void abort();

   virtual void resetInstances() { m_instances.kill(); }
   virtual void addInstance(const char* tagName, const char* name);
   virtual const IArrayOf<IPropertyTree>& getInstances() const { return m_instances; }
    static  bool stripTrailingDirsFromUNCPath(const char* uncPath, StringBuffer& netPath);
    virtual int getInstallFileCount();
    virtual offset_t getInstallFileSize();

protected:
   void checkAbort(IDeployTask* task=NULL) const;
   void checkBuild() const;
   virtual void checkInstance(IPropertyTree& node) const;
   virtual void deployInstance(IPropertyTree& node, bool useTempDir);
   virtual void stopInstance(IPropertyTree& node, const char* fileName="stop");
   virtual void startInstance(IPropertyTree& node, const char* fileName="startup");
   virtual void _deploy(bool useTempDir);
   virtual void xslTransform(const char *xsl, const char *outputFile, const char *instanceName,
        EnvMachineOS os=MachineOsUnknown, const char* processName=NULL,bool isEspModuleOrPlugin=false);
   virtual void processCustomMethod(const char *method, const char *source, const char *outputFile, 
                                    const char *instanceName, EnvMachineOS os);
   virtual void siteCertificate(IPropertyTree& process, const char *instanceName, const char *outputFile);

   StringBuffer getHostRoot(const char* computer, const char* dir, bool bIgnoreDepToFolder=false) const;
   StringBuffer getHostDir(IPropertyTree& node, bool bIgnoreDepToFolder=false);
   StringBuffer getDeployDir(IPropertyTree& node);
   StringBuffer getLocalDir(IPropertyTree& node) const;
   const char *queryDirectory(IPropertyTree& node, StringBuffer& dir) const;

   void connectToHost(IPropertyTree& node, const char* dir=NULL);
    void disconnectHost(const char* uncPath);
    void connectToNetworkPath(const char* uncPath, const char* user, const char* pswd);
   void ensurePath(const char* filespec) const;
   void renameDir(const char* from, const char* to, EnvMachineOS os);
   void backupDir(const char* from);
   void writeFile(const char* filename, const char* str, EnvMachineOS os=MachineOsUnknown);
   void deleteFile(const char* target, const char* instanceName, EnvMachineOS os);

   virtual int  determineInstallFiles(IPropertyTree& node, CInstallFiles& installFiles) const;
   virtual void copyInstallFiles(IPropertyTree& instanceNode, const char* destPath);

   virtual void beforeDeploy();
   virtual void afterDeploy() {}
   virtual void beforeDeployInstance(IPropertyTree& instanceNode, const char* destPath);
   virtual void afterDeployInstance(IPropertyTree& instanceNode, const char* destPath) {}
   bool checkFileExists(const char* filename) const;

    IPropertyTree *lookupProcess(const char* type, const char *name) const;
   IPropertyTree* lookupTable(IPropertyTree* modelTree, const char* table) const;
   StringBuffer& getEndPoints(const char* path, const char* delimiter, StringBuffer &endPoints) const;
   StringBuffer& getDaliServers(StringBuffer &daliServers) const;
   StringBuffer& getHMonIP(StringBuffer &hmon) const;
   void copyAttributes(IPropertyTree *dst, IPropertyTree *src, const char** begin, const char** end);
   void copyUnknownAttributes(IPropertyTree *dst, IPropertyTree *src, const char** begin, const char** end);

   void compareFiles(EnvMachineOS os);
    virtual const char* setCompare(const char *filename);

   const CInstallFiles& getInstallFiles() const { return m_installFiles; }
   virtual bool processInstallFile(IPropertyTree& processNode, const char* instanceName, const char* method, 
                                   const char* source, const char* dest,  
                                   EnvMachineOS os, bool bCacheable, 
                                   const char* params=NULL);
   static void addDeploymentFile(StringBuffer &ret, const char *in, IXslTransform*);
   static void siteCertificateFunction(StringBuffer &ret, const char *in, IXslTransform*);
   virtual IEnvDeploymentEngine& getEnvDepEngine() const { return m_envDepEngine; }
   virtual IDeploymentCallback& getCallback() const { return *m_pCallback; }
     void copyInstallFiles(const char* instanceName, int instanceIndex, const char* destPath, EnvMachineOS os);

   //override for IExceptionHandler
   bool fireException(IException *e);

private:
    virtual void compareFiles(const char *newFile, const char *oldFile, EnvMachineOS os);
   void createIniFile(const char* destPath, EnvMachineOS os);
    IPropertyTree* queryBuildSetNode(IPropertyTree& processNode, IPropertyTree*& buildNode) const;
    IPropertyTree* getDeployMapNode(IPropertyTree* buildNode, IPropertyTree* buildSetNode) const;
    bool searchDeployMap(const char* fileName, const char* optionalFileExt) const;
    void getBackupDirName(const char* from, StringBuffer& to);
    bool checkSSHFileExists(const char* dir) const;
    void setSSHVars(IPropertyTree& instance);
    void clearSSHVars();

protected:
   Owned<IThreadPool>      m_threadPool;
   Owned<IDeploymentCallback> m_pCallback;
   IEnvDeploymentEngine&   m_envDepEngine;
   Owned<IXslFunction>     m_externalFunction;
   Owned<IXslFunction>     m_externalFunction2;
   IConstEnvironment&      m_environment;   
   Owned<IPropertyTree>    m_rootNode;
    IPropertyTree&          m_process;
    IArrayOf<IPropertyTree> m_instances;
    set<string>                 m_connections;
    IArrayOf<IDeployTask>   m_renameDirList;
   CInstallFiles           m_installFiles;
   static CInstallFileList s_dynamicFileList;//additional files like plugin files determined at deployment time
    IXslProcessor*          m_processor;
    IXslTransform*          m_transform;
   const char*             m_curInstance;
   CriticalSection         m_critSec;
   static CDeploymentEngine* s_xsltDepEngine;//deployment engine context for XSLT
   static bool s_bCacheableDynFile;

    enum ThreeState {no, yes, unknown};
    mutable ThreeState m_startable;
    mutable ThreeState m_stoppable;
    
    StringAttr m_name;
    StringAttr m_instanceType;
    StringAttr m_compareOld;
    StringAttr m_compareNew;
    StringAttr m_iniFile;
    StringAttr m_cachePath;
    StringAttr m_curSSHKeyFile;
    StringAttr m_curSSHUser;
    StringAttr m_curSSHKeyPassphrase;
    unsigned   m_deployFlags;
    bool       m_compare;
    bool       m_createIni;
    bool       m_abort;
    bool m_useSSHIfDefined;
    bool m_instanceCheck;
};


//---------------------------------------------------------------------------
#endif // DEPLOYMENTENGINE_HPP_INCL

