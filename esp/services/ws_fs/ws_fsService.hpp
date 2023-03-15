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

#ifndef _ESPWIZ_FileSpray_HPP__
#define _ESPWIZ_FileSpray_HPP__

#include "ws_fs_esp.ipp"
#include "msgbuilder.hpp"
#include "jthread.hpp"
#include "dfuwu.hpp"
#include "environment.hpp"
#include "TpWrapper.hpp"

class Schedule : public Thread
{
    bool stopping;
    Semaphore semSchedule;
    IEspContainer* m_container;
    bool detached;
public:
    Schedule()
    {
        stopping = false;
        detached = false;
        m_container = nullptr;
    };
    ~Schedule()
    {
        stopping = true;
        semSchedule.signal();
        join();
    }

    virtual int run();
    virtual void setContainer(IEspContainer * container)
    {
        m_container = container;
        if (m_container)
          setDetachedState(!m_container->isAttachedToDali());
    }

    void setDetachedState(bool detached_)
    {
        if (detached != detached_)
        {
            detached = detached_;
            semSchedule.signal();
        }
    }

};

class CFileSprayEx : public CFileSpray
{
    void readAndCheckSpraySourceReq(MemoryBuffer& srcxml, const char* srcIP, const char* srcPath, const char* srcplane,
        StringBuffer& sourceIPReq, StringBuffer& sourcePathReq);
    void getServersInDropZone(const char* dropZoneName, IArrayOf<IConstTpDropZone>& dropZoneList,
        bool isECLWatchVisibleOnly, StringArray& serverList);
    IPropertyTree* getAndValidateDropZone(const char * path, const char * host);
    void checkDZScopeAccessAndSetSpraySourceDFUFileSpec(IEspContext& context, const char* srcPlane, const char * srcHost,
        const char* srcFile, MemoryBuffer& srcXML, IDFUfileSpec* srcDFUfileSpec);
    void validateDropZoneScopePermissionsByDFUFileSpec(IEspContext& context, const char* dropZoneName, IDFUfileSpec* fileSpec,
        SecAccessFlags permissionReq);

public:
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual void setContainer(IEspContainer * container)
    {
        CFileSpray::setContainer(container);
        m_sched.setContainer(container);
    }

    virtual bool onEchoDateTime(IEspContext &context, IEspEchoDateTime &req, IEspEchoDateTimeResponse &resp)
    {
        resp.setResult("0000-00-00T00:00:00");
        return true;
    }

    virtual bool onShowResult(IEspContext &context, IEspShowResultRequest &req, IEspShowResultResponse &resp)
    {
        resp.setResult(req.getResult());
        return true;
    }

    bool attachServiceToDali() override
    {
        m_sched.setDetachedState(false);
        return true;
    }

    bool detachServiceFromDali() override
    {
        m_sched.setDetachedState(true);
        return true;
    }


    virtual bool onDFUWUSearch(IEspContext &context, IEspDFUWUSearchRequest & req, IEspDFUWUSearchResponse & resp);
    virtual bool onGetDFUWorkunits(IEspContext &context, IEspGetDFUWorkunits &req, IEspGetDFUWorkunitsResponse &resp);
    virtual bool onGetDFUWorkunit(IEspContext &context, IEspGetDFUWorkunit &req, IEspGetDFUWorkunitResponse &resp);
    virtual bool onCreateDFUWorkunit(IEspContext &context, IEspCreateDFUWorkunit &req, IEspCreateDFUWorkunitResponse &resp);
    virtual bool onUpdateDFUWorkunit(IEspContext &context, IEspUpdateDFUWorkunit &req, IEspUpdateDFUWorkunitResponse &resp);
    virtual bool onDeleteDFUWorkunits(IEspContext &context, IEspDeleteDFUWorkunits &req, IEspDeleteDFUWorkunitsResponse &resp);
    virtual bool onDeleteDFUWorkunit(IEspContext &context, IEspDeleteDFUWorkunit &req, IEspDeleteDFUWorkunitResponse &resp);
    virtual bool onSubmitDFUWorkunit(IEspContext &context, IEspSubmitDFUWorkunit &req, IEspSubmitDFUWorkunitResponse &resp);
    virtual bool onAbortDFUWorkunit(IEspContext &context, IEspAbortDFUWorkunit &req, IEspAbortDFUWorkunitResponse &resp);
    virtual bool onGetDFUExceptions(IEspContext &context, IEspGetDFUExceptions &req, IEspGetDFUExceptionsResponse &resp);

    virtual bool onSprayFixed(IEspContext &context, IEspSprayFixed &req, IEspSprayFixedResponse &resp);
    virtual bool onSprayVariable(IEspContext &context, IEspSprayVariable &req, IEspSprayResponse &resp);
    virtual bool onReplicate(IEspContext &context, IEspReplicate &req, IEspReplicateResponse &resp);
    virtual bool onDespray(IEspContext &context, IEspDespray &req, IEspDesprayResponse &resp);
    virtual bool onCopy(IEspContext &context, IEspCopy &req, IEspCopyResponse &resp);
    virtual bool onRename(IEspContext &context, IEspRename &req, IEspRenameResponse &resp);
    virtual bool onDFUWUFile(IEspContext &context, IEspDFUWUFileRequest &req, IEspDFUWUFileResponse &resp);
    virtual bool onFileList(IEspContext &context, IEspFileListRequest &req, IEspFileListResponse &resp);
    virtual bool onDFUWorkunitsAction(IEspContext &context, IEspDFUWorkunitsActionRequest &req, IEspDFUWorkunitsActionResponse &resp);
    virtual bool onDfuMonitor(IEspContext &context, IEspDfuMonitorRequest &req, IEspDfuMonitorResponse &resp);
    virtual bool onGetDFUProgress(IEspContext &context, IEspProgressRequest &req, IEspProgressResponse &resp);
    virtual bool onOpenSave(IEspContext &context, IEspOpenSaveRequest &req, IEspOpenSaveResponse &resp);
    virtual bool onDropZoneFileSearch(IEspContext &context, IEspDropZoneFileSearchRequest &req, IEspDropZoneFileSearchResponse &resp);
    virtual bool onDropZoneFiles(IEspContext &context, IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp);
    virtual bool onDeleteDropZoneFiles(IEspContext &context, IEspDeleteDropZoneFilesRequest &req, IEspDFUWorkunitsActionResponse &resp);
    virtual bool onGetSprayTargets(IEspContext &context, IEspGetSprayTargetsRequest &req, IEspGetSprayTargetsResponse &resp);
    virtual bool onGetDFUServerQueues(IEspContext &context, IEspGetDFUServerQueuesRequest &req, IEspGetDFUServerQueuesResponse &resp);

protected:
    StringBuffer m_QueueLabel;
    StringBuffer m_MonitorQueueLabel;
    Schedule m_sched;
    Owned<IPropertyTree> directories;

    void addToQueryString(StringBuffer &queryString, const char *name, const char *value);
    void getInfoFromSasha(IEspContext &context, const char *sashaServer, const char* wuid, IEspDFUWorkunit *info);
    bool getArchivedWUInfo(IEspContext &context, IEspGetDFUWorkunit &req, IEspGetDFUWorkunitResponse &resp);
    bool GetArchivedDFUWorkunits(IEspContext &context, IEspGetDFUWorkunits &req, IEspGetDFUWorkunitsResponse &resp);
    void getPhysicalFiles(IEspContext &context, const char *dropZoneName, const char *host, const char *path, const char *fileNameMask, bool directoryOnly, IArrayOf<IConstPhysicalFileStruct> &files);
    bool ParseLogicalPath(const char * pLogicalPath, StringBuffer &title);
    bool ParseLogicalPath(const char * pLogicalPath, const char *group, const char* cluster, StringBuffer &folder, StringBuffer &title, StringBuffer &defaultFolder, StringBuffer &defaultReplicateFolder);
    StringBuffer& getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage);
    void appendGroupNode(IArrayOf<IEspGroupNode>& groupNodes, const char* nodeName, const char* clusterType, bool replicateOutputs);
    bool getOneDFUWorkunit(IEspContext& context, const char* wuid, IEspGetDFUWorkunitsResponse& resp);
    void getDropZoneInfoByDestPlane(double clientVersion, const char* destGroup, const char* destFileIn, StringBuffer& destFileOut, StringBuffer& umask, StringBuffer & hostip);
    bool checkDropZoneIPAndPath(double clientVersion, const char* dropZone, const char* netAddr, const char* path);
    void addPhysicalFile(IEspContext& context, IDirectoryIterator* di, const char* name, const char* path, const char* server, IArrayOf<IConstPhysicalFileStruct>&files);
    bool searchDropZoneFiles(IEspContext& context, const char* dropZone, const char* server, const char* dir, const char* relDir, const char* nameFilter, IArrayOf<IConstPhysicalFileStruct>& files, unsigned& filesFound);
    void setDFUServerQueueReq(const char* dfuServerQueue, IDFUWorkUnit* wu);
    void setUserAuth(IEspContext &context, IDFUWorkUnit* wu);
    void checkDropZoneFileScopeAccess(IEspContext &context, const char *dropZoneName, const char *netAddress,
        const char *dropZonePath, const StringArray &dropZoneFiles, SecAccessFlags accessReq);
};

#endif //_ESPWIZ_FileSpray_HPP__

