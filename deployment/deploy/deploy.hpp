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
#ifndef DEPLOY_HPP_INCL
#define DEPLOY_HPP_INCL

#ifdef DEPLOY_EXPORTS
  #define DEPLOY_API DECL_EXPORT
#else
  #define DEPLOY_API DECL_IMPORT
#endif

//disable the harmless warning about very long symbol names > 255 chars in debug mode
//this is typical with STL
#pragma warning( disable : 4786 )


#include "jiface.hpp"
#include "jthread.hpp"
#include "environment.hpp"
interface IPropertyTree;
interface IXslProcessor;
interface IXslTransform;
interface IConstEnvironment;
interface IEnvDeploymentEngine;
interface IDeploymentCallback;
class StringAttr;

enum
{
    DT_SOURCE=0,
    DT_TARGET=1
};

#define DTC_SIZE      0x01     // compare file size
#define DTC_TIME      0x02     // compare file timestamp
#define DTC_CRC       0x04     // compare file crc
#define DTC_ALL       (DTC_SIZE | DTC_TIME | DTC_CRC)
#define DTC_DEL_WRONG_CASE 0x08// delete files with incorrect file name case


interface IDeployTask : extends IInterface
{
    virtual const char* getCaption() const = 0;
    virtual const char* getCompName() const = 0;
    virtual const char* getInstanceName() const = 0;
    virtual const char* getFileSpec(int idx) const = 0;
    virtual const char* getFileName(int idx) const = 0;
    virtual const char* getFileExt(int idx) const = 0;
    virtual StringAttr getFilePath(int idx) const = 0;
    virtual bool getFileExists(int idx) const = 0;
    virtual offset_t getFileSize(int idx) const = 0;
    virtual EnvMachineOS getMachineOS() const = 0;
    virtual const char* getSSHUser() const = 0;
    virtual const char* getSSHKeyFile() const = 0;
    virtual bool isProcessed() const = 0;
    virtual DWORD getErrorCode() const = 0;
    virtual const char* getErrorString() const = 0;
    virtual const char* getWarnings() const = 0;
    virtual void setErrorCode(DWORD) = 0;
    virtual void setErrorString(const char*) = 0;
    virtual void setWarnings(const char*) = 0;
    virtual void setProcessed(bool bProcessed = true) = 0;
    virtual void setCaption(const char* caption) = 0;
    virtual void setFileSpec(int idx, const char* file) = 0;
    
    virtual bool getAbort() const = 0;
    virtual IDeploymentCallback& getCallback() const = 0;
    
    virtual unsigned getFlags() const = 0;
    virtual void setFlags(unsigned)   = 0;
    virtual void setUpdateProgress(bool flag) = 0;
    
    virtual bool createFile(const char* text) = 0;
    virtual bool transformFile(IXslProcessor& processor, IXslTransform& transform, const char* tempPath) = 0;
    virtual bool copyFile(unsigned mode) = 0;
    virtual bool renameFile() = 0;
    virtual bool deleteFile() = 0;
    virtual bool compareFile(unsigned mode) = 0;
    virtual bool createDirectory() = 0;
    virtual bool copyDirectory() = 0;
    virtual bool connectTarget(const char* user, const char* pwd, bool bInteractiveMode=true) = 0;
    virtual bool disconnectTarget() = 0;
    virtual bool createProcess(bool wait, const char* user=NULL, const char* pwd=NULL) = 0;
    virtual bool execSSHCmd(const char* ip, const char* cmd, StringBuffer& output, StringBuffer& errmsg) = 0;
};

interface IDeployLog : extends IInterface
{
    virtual IPropertyTree* addTask(IDeployTask& task) = 0;
    virtual IPropertyTree* addDirList(const char* comp, const char* path) = 0;
};

interface IDeployTaskThread : extends IPooledThread
{
    virtual void setTask(IDeployTask*)   = 0;
    virtual IDeployTask* getTask() const = 0;
};
const unsigned int DEPLOY_THREAD_POOL_SIZE = 10;

#define DEFLAGS_NONE          0x00
#define DEFLAGS_CONFIGFILES   0x01
#define DEFLAGS_BUILDFILES    0x02
#define DEFLAGS_ALL           0x03

//deploy conditions - can be mixed with DEFLAGS above
#define DCFLAGS_NONE          0x00
#define DCFLAGS_TIME          (DTC_SIZE << 2)
#define DCFLAGS_SIZE          (DTC_TIME << 2)
#define DCFLAGS_CRC           (DTC_CRC  << 2)
#define DCFLAGS_ALL           (DCFLAGS_TIME | DCFLAGS_SIZE | DCFLAGS_CRC)

enum BackupMode { DEBACKUP_NONE=0, DEBACKUP_COPY, DEBACKUP_RENAME };
enum StatusType { STATUS_NORMAL, STATUS_INCOMPLETE, STATUS_OK, STATUS_ERROR, STATUS_WARN };


interface IDeploymentEngine : extends IInterface
{
    virtual void setXsl(IXslProcessor* processor, IXslTransform* transform) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual void check() = 0;
    virtual void compare(unsigned flags) = 0;
    virtual void deploy(unsigned flags, bool useTempDir) = 0;
    virtual void renameDirs() = 0;
    virtual void backupDirs() = 0;
    virtual void abort() = 0;
    virtual void resetInstances() = 0;
    virtual void addInstance(const char* tagName, const char* name) = 0;
    virtual const IArrayOf<IPropertyTree>& getInstances() const = 0;
    virtual IEnvDeploymentEngine& getEnvDepEngine() const = 0;
    virtual IDeploymentCallback& getCallback() const = 0;
    virtual int getInstallFileCount() = 0;
    virtual offset_t getInstallFileSize() = 0;
};

interface IDeploymentCallback : extends IInterface
{
    virtual void printStatus(IDeployTask* task) = 0;
    virtual void printStatus(StatusType type, const char* processType, const char* comp, 
        const char* instance, const char* msg=NULL, ...) __attribute__((format(printf, 6, 7))) = 0;
    virtual bool onDisconnect(const char* target) = 0;
    virtual bool getAbortStatus() const = 0;
    virtual void setAbortStatus(bool bAbort) = 0;
    virtual void setEnvironmentUpdated() = 0;
    virtual void getSshAccountInfo(StringBuffer& userid, StringBuffer& password) const = 0;
    //the following throws exception on abort, returns true for ignore
    virtual bool processException(const char* processType, const char* process, const char* instance, 
        IException* e, const char* szMessage=NULL, const char* szCaption=NULL,
        IDeployTask* pTask = NULL ) = 0;
    virtual IEnvDeploymentEngine* getEnvDeploymentEngine() const = 0;
    virtual void* getWindowHandle() const = 0;
    virtual void installFileListChanged() = 0;
    virtual void fileSizeCopied(offset_t size, bool bWholeFileDone) = 0;
};

interface IEnvDeploymentEngine : extends IInterface
{
    virtual IConstEnvironment& getEnvironment() const = 0;
    virtual IDeploymentEngine* addProcess(const char* processType, const char* processName) = 0;
    virtual void setInteractiveMode(bool bSet) = 0;
    virtual bool getInteractiveMode() const = 0;
    virtual void archive(const char* filename) = 0;
    virtual void setLog(const char* filename, const char* envname) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual void check() = 0;
    virtual void compare(unsigned flags) = 0;
    virtual void deploy(unsigned flags, BackupMode backupMode, bool bStop, bool bStart) = 0;
    virtual void abort() = 0;
    virtual int  incrementTempFileCount() = 0;
    virtual int  incrementEspModuleCount() = 0;
    virtual bool isLinuxDeployment() const = 0;
    virtual IDeploymentCallback& getCallback() const = 0;
    virtual IDeployLog* getDeployLog() = 0;
    virtual void addTempFile(const char* filePath) = 0;
    virtual void addTempDirectory(const char* dirPath) = 0;
    virtual void setDeployToFolder(const char*) = 0;
    virtual StringAttr& lookupNetAddress(StringAttr& str, const char* computer) const = 0;
    virtual EnvMachineOS  lookupMachineOS(IPropertyTree& node) const = 0;
    virtual void getAccountInfo(const char* computer, StringAttr& user, StringAttr& pwd) const = 0;
    virtual const char* getDeployToFolder() const = 0;
    virtual void getDeployToAccountInfo(const char*& user, const char*& pswd) const = 0;
    virtual void setSourceDaliAddress( const char* addr ) = 0;
    virtual const char* getSourceDaliAddress() = 0;
    virtual IArrayOf<IDeploymentEngine>& queryProcesses() = 0;
    virtual bool IsPersistentConnection(const char* path) const = 0;
    virtual void getSSHAccountInfo(const char* computer, StringAttr& user, StringAttr& sshKey, StringAttr& sshKeyPassphrase) const = 0;
};

//---------------------------------------------------------------------------
// Factory functions
//---------------------------------------------------------------------------
extern DEPLOY_API IEnvDeploymentEngine* createEnvDeploymentEngine(IConstEnvironment& environment, 
                                                                  IDeploymentCallback& callback,
                                                                  IPropertyTree* pSelectedComponents);
extern DEPLOY_API IDeployTask* createDeployTask(IDeploymentCallback& callback, const char* caption, 
                                                const char* processType, const char* comp, const char* instance, 
                                                const char* source, const char* target, 
                                                const char* sshUser, const char* sshPubKeyFile, 
                                                const char* sshPubKeyPassphrase, bool useSsh, 
                                                EnvMachineOS os = MachineOsUnknown, 
                                                const char* processName=NULL);
extern DEPLOY_API IDeployLog* createDeployLog(IDeploymentCallback& callback, const char* filename, const char* envFilename);
extern DEPLOY_API IThreadFactory* createDeployTaskThreadFactory();
extern DEPLOY_API IEnvDeploymentEngine* createConfigGenMgr(IConstEnvironment& env, 
                                                           IDeploymentCallback& callback,
                                                           IPropertyTree* pSelectedComponents,
                                                           const char* inputDir,
                                                           const char* outputDir,
                                                           const char* compName,
                                                           const char* compType,
                                                           const char* ipAddr);

extern DEPLOY_API IPropertyTree* getInstances(const IPropertyTree* pEnvRoot, 
                                              const char* compName,
                                              const char* compType,
                                              const char* ipAddr,
                                              bool listall=false);

extern DEPLOY_API bool matchDeployAddress(const char* searchIP, const char *envIP);

//---------------------------------------------------------------------------
// Module globals 
//---------------------------------------------------------------------------
extern const char* findFileExtension(const char* pszPath);
extern void removeTrailingPathSepChar(char* pszPath);
extern void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir=NULL);
extern DWORD getLastError();
extern void initializeMultiThreadedCopying();
extern void stripNetAddr(const char* dir, StringBuffer& destpath, StringBuffer& destip, bool makeLinux=true);

#endif // DEPLOYENV_HPP_INCL
