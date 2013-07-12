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

#pragma warning (disable : 4786)
#pragma warning (disable : 4297)  // function assumed not to throw an exception but does

#include "platform.h"
#include "fileservices.hpp"
#include "workunit.hpp"
#include "agentctx.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "daft.hpp"
#include "dasess.hpp"
#include "dautils.hpp"
#include "daaudit.hpp"
#include "dfuwu.hpp"
#include "ws_fs_esp.ipp"
#include "rmtsmtp.hpp"
#include "dfuplus.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "enginecontext.hpp"

#define USE_DALIDFS
#define SDS_LOCK_TIMEOUT  10000

#define FILESERVICES_VERSION "FILESERVICES 2.1.3"

static const char * compatibleVersions[] = {
    "FILESERVICES 2.1  [a68789cfb01d00ef6dc362e52d5eac0e]", // linux version
    "FILESERVICES 2.1.1",
    "FILESERVICES 2.1.2",
    "FILESERVICES 2.1.3",
    NULL };

static const char * EclDefinition =
"export FsFilenameRecord := record string name{maxlength(1023)}; integer8 size; string19 modified; end; \n"
"export FsLogicalFileName := string{maxlength(255)}; \n"
"export FsLogicalFileNameRecord := record FsLogicalFileName name; end; \n"
"export FsLogicalFileInfoRecord := record(FsLogicalFileNameRecord) boolean superfile; integer8 size; integer8 rowcount; string19 modified; string owner{maxlength(255)}; string cluster{maxlength(255)}; end; \n"
"export FsLogicalSuperSubRecord := record string supername{maxlength(255)}; string subname{maxlength(255)}; end; \n"
"export FsFileRelationshipRecord := record  string primaryfile   {maxlength(1023)}; string secondaryfile {maxlength(1023)}; string primaryflds   {maxlength(1023)}; string secondaryflds {maxlength(1023)}; string kind {maxlength(16)}; string cardinality   {maxlength(16)}; boolean payload; string description   {maxlength(1023)}; end; \n"
"export integer4 RECFMV_RECSIZE := -2; // special value for SprayFixed record size \n"
"export integer4 RECFMVB_RECSIZE := -1; // special value for SprayFixed record size \n"
"export integer4 PREFIX_VARIABLE_RECSIZE := -3; // special value for SprayFixed record size \n"
"export integer4 PREFIX_VARIABLE_BIGENDIAN_RECSIZE := -4; // special value for SprayFixed record size \n"
"export FileServices := SERVICE\n"
"  boolean FileExists(const varstring lfn, boolean physical=false) : c,context,entrypoint='fsFileExists'; \n"
"  DeleteLogicalFile(const varstring lfn,boolean ifexists=false) : c,action,context,entrypoint='fsDeleteLogicalFile'; \n"
"  SetReadOnly(const varstring lfn, boolean ro) : c,action,context,entrypoint='fsSetReadOnly'; \n"
"  RenameLogicalFile(const varstring oldname, const varstring newname) : c,action,context,entrypoint='fsRenameLogicalFile'; \n"
"  varstring GetBuildInfo() : c,pure,entrypoint='fsGetBuildInfo';\n"
"  SendEmail(const varstring to, const varstring subject, const varstring body, const varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), const varstring sender=GETENV('emailSenderAddress')) : c,action,context,entrypoint='fsSendEmail'; \n"
"  SendEmailAttachText(const varstring to, const varstring subject, const varstring body, const varstring attachment, const varstring mimeType, const varstring attachmentName, const varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), const varstring sender=GETENV('emailSenderAddress')) : c,action,context,entrypoint='fsSendEmailAttachText'; \n"
"  SendEmailAttachData(const varstring to, const varstring subject, const varstring body, const data attachment, const varstring mimeType, const varstring attachmentName, const varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), const varstring sender=GETENV('emailSenderAddress')) : c,action,context,entrypoint='fsSendEmailAttachData'; \n"
"  varstring CmdProcess(const varstring prog, const varstring src) : c,action,entrypoint='fsCmdProcess'; \n"
"  string CmdProcess2(const varstring prog, const string src) : c,action,entrypoint='fsCmdProcess2'; \n"
"  SprayFixed(const varstring sourceIP, const varstring sourcePath, integer4 recordSize, const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false,boolean compress=false, boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsSprayFixed'; \n"
"  SprayVariable(const varstring sourceIP, const varstring sourcePath, integer4 sourceMaxRecordSize=8192, const varstring sourceCsvSeparate='\\\\,', const varstring sourceCsvTerminate='\\\\n,\\\\r\\\\n', const varstring sourceCsvQuote='\\'', const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false,boolean compress=false,const varstring sourceCsvEscape='', boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsSprayVariable2'; \n"
"  SprayXml(const varstring sourceIP, const varstring sourcePath, integer4 sourceMaxRecordSize=8192, const varstring sourceRowTag, const varstring sourceEncoding='utf8', const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false,boolean compress=false, boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsSprayXml'; \n"
"  Despray(const varstring logicalName, const varstring destinationIP, const varstring destinationPath, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false) : c,action,context,entrypoint='fsDespray'; \n"
"  Copy(const varstring sourceLogicalName, const varstring destinationGroup, const varstring destinationLogicalName, const varstring sourceDali='', integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean compress=false, boolean forcePush=false, integer4 transferBufferSize=0) : c,action,context,entrypoint='fsCopy'; \n"
"  Replicate(const varstring logicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsReplicate'; \n"
"  CreateSuperFile(const varstring lsuperfn, boolean sequentialparts=false,boolean ifdoesnotexist=false) : c,action,context,entrypoint='fsCreateSuperFile'; \n"
"  boolean SuperFileExists(const varstring lsuperfn) : c,context,entrypoint='fsSuperFileExists'; \n"
"  DeleteSuperFile(const varstring lsuperfn,boolean deletesub=false) : c,action,context,entrypoint='fsDeleteSuperFile'; \n"
"  unsigned4 GetSuperFileSubCount(const varstring lsuperfn) : c,context,entrypoint='fsGetSuperFileSubCount'; \n"
"  varstring GetSuperFileSubName(const varstring lsuperfn,unsigned4 filenum,boolean abspath=false) : c,context,entrypoint='fsGetSuperFileSubName'; \n"
"  unsigned4 FindSuperFileSubName(const varstring lsuperfn,const varstring lfn) : c,context,entrypoint='fsFindSuperFileSubName'; \n"
"  StartSuperFileTransaction() : c,action,globalcontext,entrypoint='fsStartSuperFileTransaction'; \n"
"  AddSuperFile(const varstring lsuperfn,const varstring lfn,unsigned4 atpos=0,boolean addcontents=false, boolean strict=false) : c,action,globalcontext,entrypoint='fsAddSuperFile'; \n"
"  RemoveSuperFile(const varstring lsuperfn,const varstring lfn,boolean del=false,boolean remcontents=false) : c,action,globalcontext,entrypoint='fsRemoveSuperFile'; \n"
"  ClearSuperFile(const varstring lsuperfn,boolean del=false) : c,action,globalcontext,entrypoint='fsClearSuperFile'; \n"
"  SwapSuperFile(const varstring lsuperfn1,const varstring lsuperfn2) : c,action,globalcontext,entrypoint='fsSwapSuperFile'; \n"
"  ReplaceSuperFile(const varstring lsuperfn,const varstring lfn,const varstring bylfn) : c,action,globalcontext,entrypoint='fsReplaceSuperFile'; \n"
"  FinishSuperFileTransaction(boolean rollback=false) : c,action,globalcontext,entrypoint='fsFinishSuperFileTransaction'; \n"
"  varstring ForeignLogicalFileName(const varstring name, const varstring foreigndali='', boolean abspath=false) : c,context,entrypoint='fsForeignLogicalFileName'; \n"
"  varstring WaitDfuWorkunit(const varstring wuid, integer4 timeOut=-1,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,globalcontext,entrypoint='fsWaitDfuWorkunit'; \n"
"  AbortDfuWorkunit(const varstring wuid,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,globalcontext,entrypoint='fsAbortDfuWorkunit'; \n"
"  MonitorLogicalFileName(const varstring eventname, const varstring name, integer4 shotcount=1,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsMonitorLogicalFileName'; \n"
"  MonitorFile(const varstring eventname, const varstring ip, const varstring filename, boolean subdirs=false, integer4 shotcount=1,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsMonitorFile'; \n"
"  varstring fSprayFixed(const varstring sourceIP, const varstring sourcePath, integer4 recordSize, const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsfSprayFixed'; \n"
"  varstring fSprayVariable(const varstring sourceIP, const varstring sourcePath, integer4 sourceMaxRecordSize=8192, const varstring sourceCsvSeparate='\\\\,', const varstring sourceCsvTerminate='\\\\n,\\\\r\\\\n', const varstring sourceCsvQuote='\\'', const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false,const varstring sourceCsvEscape='', boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsfSprayVariable2'; \n"
"  varstring fSprayXml(const varstring sourceIP, const varstring sourcePath, integer4 sourceMaxRecordSize=8192, const varstring sourceRowTag, const varstring sourceEncoding='utf8', const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean compress=false, boolean failIfNoSourceFile=false) : c,action,context,entrypoint='fsfSprayXml'; \n"
"  varstring fDespray(const varstring logicalName, const varstring destinationIP, const varstring destinationPath, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false) : c,action,context,entrypoint='fsfDespray'; \n"
"  varstring fCopy(const varstring sourceLogicalName, const varstring destinationGroup, const varstring destinationLogicalName, const varstring sourceDali='', integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server'), integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false, boolean compress=false, boolean forcePush=false, integer4 transferBufferSize=0) : c,action,context,entrypoint='fsfCopy'; \n"
"  varstring fMonitorLogicalFileName(const varstring eventname, const varstring name, integer4 shotcount=1,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsfMonitorLogicalFileName'; \n"
"  varstring fMonitorFile(const varstring eventname, const varstring ip, const varstring filename, boolean subdirs=false, integer4 shotcount=1,const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsfMonitorFile'; \n"
"  varstring fReplicate(const varstring logicalName, integer4 timeOut=-1, const varstring espServerIpPort=GETENV('ws_fs_server')) : c,action,context,entrypoint='fsfReplicate'; \n"
"  varstring GetFileDescription(const varstring lfn) : c,context,entrypoint='fsGetFileDescription'; \n"
"  SetFileDescription(const varstring lfn,const varstring val) : c,action,context,entrypoint='fsSetFileDescription'; \n"
"  dataset(FsFilenameRecord) RemoteDirectory(const varstring machineIP,const varstring dir,const varstring mask='*',boolean sub=false) : c,entrypoint='fsRemoteDirectory';\n"
"  dataset(FsLogicalFileInfoRecord) LogicalFileList(const varstring namepattern='*',boolean includenormal=true,boolean includesuper=false,boolean unknownszero=false,const varstring foreigndali='') : c,context,entrypoint='fsLogicalFileList';\n"
"  dataset(FsLogicalFileNameRecord) SuperFileContents(const varstring lsuperfn,boolean recurse=false) : c,context,entrypoint='fsSuperFileContents';\n"
"  dataset(FsLogicalFileNameRecord) LogicalFileSuperOwners(const varstring lfn) : c,context,entrypoint='fsLogicalFileSuperOwners';\n"
"  varstring ExternalLogicalFileName(const varstring location, const varstring path,boolean abspath=true) : c,entrypoint='fsExternalLogicalFileName'; \n"
"  integer4 CompareFiles(const varstring lfn1, const varstring lfn2,boolean logicalonly=true,boolean usecrcs=false) : c,context,entrypoint='fsCompareFiles'; \n"
"  varstring VerifyFile(const varstring lfn, boolean usecrcs) : c,action,context,entrypoint='fsVerifyFile'; \n"
"  RemotePull( const varstring remoteEspFsURL, const varstring sourceLogicalName, const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false,boolean forcePush=false, integer4 transferBufferSize=0,boolean wrap=false,boolean compress=false): c,action,context,entrypoint='fsRemotePull'; \n"
"  varstring fRemotePull( const varstring remoteEspFsURL, const varstring sourceLogicalName, const varstring destinationGroup, const varstring destinationLogicalName, integer4 timeOut=-1, integer4 maxConnections=-1, boolean allowoverwrite=false, boolean replicate=false, boolean asSuperfile=false,boolean forcePush=false, integer4 transferBufferSize=0,boolean wrap=false,boolean compress=false): c,action,context,entrypoint='fsfRemotePull'; \n"
"  dataset(FsLogicalSuperSubRecord) LogicalFileSuperSubList() : c,context,entrypoint='fsLogicalFileSuperSubList';\n"
"  PromoteSuperFileList(const set of varstring lsuperfns,const varstring addhead='',boolean deltail=false,boolean createonlyonesuperfile=false,boolean reverse=false) : c,action,context,entrypoint='fsPromoteSuperFileList'; \n"
"  varstring fPromoteSuperFileList(const set of varstring lsuperfns,const varstring addhead='', boolean deltail=false,boolean createonlyonesuperfile=false, boolean reverse=false) : c,action,context,entrypoint='fsfPromoteSuperFileList'; \n"
"  unsigned8 getUniqueInteger(const varstring foreigndali='') : c,context,entrypoint='fsGetUniqueInteger'; \n"
"  AddFileRelationship(const varstring primary, const varstring secondary, const varstring primaryflds,  const varstring secondaryflds, const varstring kind='link', const varstring cardinality, boolean payload, const varstring description='') : c,action,context,entrypoint='fsAddFileRelationship'; \n"
"  dataset(FsFileRelationshipRecord) FileRelationshipList(const varstring primary, const varstring secondary, const varstring primflds='', const varstring secondaryflds='',  const varstring kind='link') : c,action,context,entrypoint='fsFileRelationshipList'; \n"
"  RemoveFileRelationship(const varstring primary,  const varstring secondary, const varstring primaryflds='', const varstring secondaryflds='',  const varstring kind='link') : c,action,context,entrypoint='fsRemoveFileRelationship'; \n"
"  varstring GetColumnMapping( const varstring LogicalFileName): c,context,entrypoint='fsfGetColumnMapping'; \n"
"  SetColumnMapping( const varstring LogicalFileName, const varstring mapping): c,context,entrypoint='fsSetColumnMapping'; \n"
"  varstring RfsQuery( const varstring server, const varstring query): c,entrypoint='fsfRfsQuery'; \n"
"  RfsAction( const varstring server, const varstring query): c,entrypoint='fsRfsAction'; \n"
"  varstring GetHostName( const varstring ipaddress ): c,entrypoint='fsfGetHostName'; \n"
"  varstring ResolveHostName( const varstring hostname ): c,entrypoint='fsfResolveHostName'; \n"
"  MoveExternalFile(const varstring location, const varstring frompath, const varstring topath): c,action,context,entrypoint='fsMoveExternalFile'; \n"
"  DeleteExternalFile(const varstring location, const varstring path): c,action,context,entrypoint='fsDeleteExternalFile'; \n"
"  CreateExternalDirectory(const varstring location, const varstring path): c,action,context,entrypoint='fsCreateExternalDirectory'; \n"
"  varstring GetLogicalFileAttribute(const varstring lfn,const varstring attrname) : c,context,entrypoint='fsfGetLogicalFileAttribute'; \n"
"  ProtectLogicalFile(const varstring lfn,boolean set=true) : c,context,entrypoint='fsProtectLogicalFile'; \n"
"  DfuPlusExec(const varstring cmdline) : c,context,entrypoint='fsDfuPlusExec'; \n"
"END;";

#define WAIT_SECONDS 30

FILESERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = FILESERVICES_VERSION;
    pb->moduleName = "lib_fileservices";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "FileServices library";

    return true;
}

namespace nsFileservices {

    IPluginContext * parentCtx = NULL;

static IConstWorkUnit * getWorkunit(ICodeContext * ctx)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    return factory->openWorkUnit(wuid, false);
}

static IWorkUnit * updateWorkunit(ICodeContext * ctx)
{
    // following bit of a kludge, as
    // 1) eclagent keeps WU locked, and
    // 2) rtti not available in generated .so's to convert to IAgentContext
    IAgentContext * actx = dynamic_cast<IAgentContext *>(ctx);
    if (actx == NULL) { // fall back to pure ICodeContext
        // the following works for thor only
        char * platform = ctx->getPlatform();
        if (strcmp(platform,"thor")==0) {
            CTXFREE(parentCtx, platform);
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            StringAttr wuid;
            wuid.setown(ctx->getWuid());
            return factory->updateWorkUnit(wuid);
        }
        CTXFREE(parentCtx, platform);
        return NULL;
    }
    return actx->updateWorkUnit();
}

static IPropertyTree *getEnvironment()
{
    Owned<IPropertyTree> env;
    if (daliClientActive()) {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
        if (conn)
            env.setown(createPTreeFromIPT(conn->queryRoot())); // we don't really need to copy here
    }
    if (!env.get())
        env.setown(getHPCCEnvironment());
    return env.getClear();
}

static const char *getEspServerURL(const char *param)
{
    if (param&&*param)
        return param;
    static StringAttr espurl;
    if (espurl.isEmpty()) {
        Owned<IPropertyTree> env = getEnvironment();
        StringBuffer tmp;
        if (env.get()) {
            Owned<IPropertyTreeIterator> iter1 = env->getElements("Software/EspProcess");
            ForEach(*iter1) {
                Owned<IPropertyTreeIterator> iter2 = iter1->query().getElements("EspBinding");
                ForEach(*iter2) {
                    Owned<IPropertyTreeIterator> iter3 = iter2->query().getElements("AuthenticateFeature");
                    ForEach(*iter3) {
                        // if any enabled feature has service ws_fs then use this binding
                        if (iter3->query().getPropBool("@authenticate")&&
                            iter3->query().getProp("@service",tmp.clear())&&
                            (strcmp(tmp.str(),"ws_fs")==0)) {
                            if (iter2->query().getProp("@protocol",tmp.clear())) {
                                tmp.append("://");
                                StringBuffer espname;
                                if (iter1->query().getProp("@name",espname)) {
                                    StringBuffer espinst;
                                    if (iter1->query().getProp("Instance[1]/@computer",espinst)) {
                                        StringBuffer ipq;
                                        if (env->getProp(ipq.appendf("Hardware/Computer[@name=\"%s\"]/@netAddress",espinst.str()).str(),tmp)) {
                                            tmp.append(':').append(iter2->query().getPropInt("@port",8010)).append("/FileSpray"); // FileSpray seems to be fixed
                                            espurl.set(tmp);
                                            PROGLOG("fileservices using esp URL: %s",espurl.get());
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!espurl.isEmpty()) 
                        break;
                }
                if (!espurl.isEmpty()) 
                    break;
            }
        }
    }
    if (espurl.isEmpty()) 
        throw MakeStringException(-1,"Cannot determine ESP Url");
    return espurl.get();
}



StringBuffer & constructLogicalName(IConstWorkUnit * wu, const char * partialLogicalName, StringBuffer & result)
{
    if (partialLogicalName == NULL)
        throw MakeStringException(0, "Logical Name Cannot be blank");

    if (*partialLogicalName == '~')
        ++partialLogicalName;
    else
    {
        StringBuffer prefix;
        wu->getScope(StringBufferAdaptor(prefix));
        if (prefix.length())
            result.append(prefix).append("::");
    }
    result.append(partialLogicalName);
    if ((result.length()>0)&&(strstr(result.str(),"::")==NULL)&&(result.charAt(0)!='#'))
        result.insert(0,".::");
    return result;
}

StringBuffer & constructLogicalName(ICodeContext * ctx, const char * partialLogicalName, StringBuffer & result)
{
    Owned<IConstWorkUnit> wu;
    if (partialLogicalName&&(*partialLogicalName != '~'))
        wu.setown(getWorkunit(ctx));
    return constructLogicalName(wu, partialLogicalName, result);
}

static void WUmessage(ICodeContext *ctx, WUExceptionSeverity sev, const char *fn, const char *msg)
{
    StringBuffer s;
    s.append("fileservices");
    if (fn)
        s.append(", ").append(fn);
    IAgentContext * actx = dynamic_cast<IAgentContext *>(ctx); // doesn't work if called from helper .so (no rtti)
    if (actx)
        actx->addWuException(msg,0,sev,s.str());
    else {
        Owned<IWorkUnit> wu = updateWorkunit(ctx);
        if (wu.get()) {
            Owned<IWUException> we = wu->createException();
            we->setSeverity(sev);
            we->setExceptionMessage(msg);
            we->setExceptionSource(s.str());
        }
        else {
            s.append(" : ").append(msg);
            ctx->addWuException(s.str(),0,sev); // use plain code context
        }
    }
}

static void AuditMessage(ICodeContext *ctx,
                         const char *func,
                         const char *lfn1,
                         const char *lfn2=NULL)
{
    // FileServices,WUID,user,function,LFN1,LFN2
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer aln;
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    aln.append(",FileAccess,FileServices,").append(func).append(',').append(wuid).append(',');
    if (udesc)
        udesc->getUserName(aln);
    if (lfn1&&*lfn1) {
        aln.append(',').append(lfn1);
        if (lfn2&&*lfn2) {
            aln.append(',').append(lfn2);
        }
    }
    LOG(daliAuditLogCat,"%s",aln.str());
}


}//namespace
using namespace nsFileservices;

FILESERVICES_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

FILESERVICES_API char * FILESERVICES_CALL fsGetBuildInfo(void)
{
    return CTXSTRDUP(parentCtx, FILESERVICES_VERSION);
}

//-------------------------------------------------------------------------------------------------------------------------------------------

FILESERVICES_API void FILESERVICES_CALL fsDeleteLogicalFile(ICodeContext *ctx, const char *name,bool ifexists)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer uname;
    PrintLog("Deleting NS logical file %s for user %s", lfn.str(),udesc?udesc->getUserName(uname).str():"");
    if (queryDistributedFileDirectory().removeEntry(lfn.str(),udesc,transaction))
    {
        StringBuffer s("DeleteLogicalFile ('");
        s.append(lfn);
        if (transaction->active())
            s.append("') added to transaction");
        else
            s.append("') done");
        WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
        AuditMessage(ctx,"DeleteLogicalFile",lfn.str());

    }
    else if (!ifexists)
    {
        throw MakeStringException(0, "Could not delete file %s", lfn.str());
    }
}


FILESERVICES_API bool FILESERVICES_CALL fsFileExists(ICodeContext *ctx, const char *name, bool physical)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);
    if (physical)
        return queryDistributedFileDirectory().existsPhysical(lfn.str(),ctx->queryUserDescriptor());

    return queryDistributedFileDirectory().exists(lfn.str(),ctx->queryUserDescriptor(),false,false);
}

FILESERVICES_API bool FILESERVICES_CALL fsFileValidate(ICodeContext *ctx, const char *name)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
    if (df)
    {
        Owned<IDistributedFilePartIterator> partIter = df->getIterator();
        ForEach(*partIter)
        {
            IDistributedFilePart & part = partIter->query();
            unsigned numCopies = part.numCopies();
            bool gotone = false;
            offset_t partSize;
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename remote;
                part.getFilename(remote,copy);
                OwnedIFile file = createIFile(remote);

                if (file->exists())
                {
                    offset_t thisSize = file->size();
                    if (gotone && (partSize != thisSize))
                        throw MakeStringException(0, "Inconsistent file sizes for %s", lfn.str());
                    partSize = thisSize;
                    gotone = true;
                }
            }
            if (!gotone)
                return false;
        }
        return true;
    }

    return false;
}

FILESERVICES_API void FILESERVICES_CALL fsSetReadOnly(ICodeContext *ctx, const char *name, bool ro)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IException> error;
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc, true);
    if (df)
    {
        Owned<IDistributedFilePartIterator> partIter = df->getIterator();
        ForEach(*partIter)
        {
            IDistributedFilePart & part = partIter->query();
            unsigned numCopies = part.numCopies();
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename remote;
                part.getFilename(remote,copy);
                OwnedIFile file = createIFile(remote);

                try
                {
                    file->setReadOnly(ro);
                }
                catch (IException * e)
                {
                    EXCLOG(e);
                    e->Release();
                }
            }
        }
        return;
    }

    if (!error)
        error.setown(MakeStringException(0, "Could not find logical file %s", lfn.str()));
    throw error.getClear();
}


FILESERVICES_API void FILESERVICES_CALL fsRenameLogicalFile(ICodeContext *ctx, const char *oldname, const char *newname)
{
    StringBuffer lfn, nlfn;
    constructLogicalName(ctx, oldname, lfn);
    constructLogicalName(ctx, newname, nlfn);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    try {
        queryDistributedFileDirectory().renamePhysical(lfn.str(),nlfn.str(),udesc,transaction);
        StringBuffer s("RenameLogicalFile ('");
        s.append(lfn).append(", '").append(nlfn).append("') done");
        WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
        AuditMessage(ctx,"RenameLogicalFile",lfn.str(),nlfn.str());
    }
    catch (IException *e)
    {
        StringBuffer s;
        e->errorMessage(s);
        WUmessage(ctx,ExceptionSeverityWarning,"RenameLogicalFile",s.str());
        throw e;
     }
}


FILESERVICES_API void FILESERVICES_CALL fsSendEmail(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender)
{
    StringArray warnings;
    sendEmail( to, subject, body, mailServer, port, sender, &warnings);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, ExceptionSeverityWarning, "SendEmail", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachText(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender)
{
    StringArray warnings;
    sendEmailAttachText(to, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, ExceptionSeverityWarning, "SendEmailAttachText", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachData(ICodeContext * ctx, const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender)
{
    StringArray warnings;
    sendEmailAttachData(to, subject, body, lenAttachment, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, ExceptionSeverityWarning, "SendEmailAttachData", warnings.item(i));
}



FILESERVICES_API char * FILESERVICES_CALL fsCmdProcess(const char *prog, const char *src)
{
    StringBuffer in, out;
    in.append(src);

    callExternalProgram(prog, in, out);

    return CTXSTRDUP(parentCtx, out.str());
}


FILESERVICES_API void FILESERVICES_CALL fsCmdProcess2(unsigned & tgtLen, char * & tgt, const char *prog, unsigned srcLen, const char * src)
{
    StringBuffer in, out;
    in.append(srcLen, src);

    callExternalProgram(prog, in, out);

    tgtLen = out.length();
    tgt = (char *)CTXDUP(parentCtx, out.str(), out.length());
}


static void blockUntilComplete(const char * label, IClientFileSpray &server, ICodeContext *ctx, const char * wuid, int timeOut, StringBuffer *stateout=NULL, bool monitoringok=false)
{
    if (!wuid || strcmp(wuid, "") == 0)
        return;

    if (timeOut == 0)
        return;

    CTimeMon time(timeOut);

    unsigned polltime = 1;

    while(true)
    {
        Owned<IWorkUnit> wu = updateWorkunit(ctx); // may return NULL

        Owned<IClientGetDFUWorkunit> req = server.createGetDFUWorkunitRequest();
        req->setWuid(wuid);
        Linked<IClientGetDFUWorkunitResponse> result = server.GetDFUWorkunit(req);

        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }

        IConstDFUWorkunit & dfuwu = result->getResult();

        if (wu.get()) { // if updatable (e.g. not hthor with no agent context)
            StringBuffer ElapsedLabel, RemainingLabel;
            ElapsedLabel.appendf("%s-%s (Elapsed) ", label, dfuwu.getID());
            RemainingLabel.appendf("%s-%s (Remaining) ", label, dfuwu.getID());
            wu->setTimerInfo(ElapsedLabel.str(), "", time.elapsed(), 1, 0);
            wu->setTimerInfo(RemainingLabel.str(), "", dfuwu.getSecsLeft()*1000, 1, 0);
            wu->setApplicationValue(label, dfuwu.getID(), dfuwu.getSummaryMessage(), true);
            wu->commit();
        }

        DFUstate state = (DFUstate)dfuwu.getState();
        if (stateout)
            stateout->clear().append(dfuwu.getStateMessage());
        switch(state)
        {
        case DFUstate_unknown:
        case DFUstate_scheduled:
        case DFUstate_queued:
        case DFUstate_started:
        case DFUstate_aborting:
            break;

        case DFUstate_monitoring:
            if (monitoringok)
                return;
            break;

        case DFUstate_aborted:
        case DFUstate_failed:
            throw MakeStringException(0, "DFUServer Error %s", dfuwu.getSummaryMessage());
            return;

        case DFUstate_finished:
            return;
        }

        if (wu.get()&&wu->aborting())
        {
            Owned<IClientAbortDFUWorkunit> abortReq = server.createAbortDFUWorkunitRequest();
            abortReq->setWuid(wuid);
            Linked<IClientAbortDFUWorkunitResponse> abortResp = server.AbortDFUWorkunit(abortReq);

            {   //  Add warning of DFU Abort Request - should this be information  ---
                StringBuffer s("DFU Workunit Abort Requested for ");
                s.append(wuid);
                WUmessage(ctx,ExceptionSeverityWarning,"blockUntilComplete",s.str());
            }

            wu->setState(WUStateAborting);
            throw MakeStringException(0, "Workunit abort request received");

        }
        wu.clear();

        if (time.timedout()) {
            unsigned left = dfuwu.getSecsLeft();
            if (left)
                throw MakeStringException(0, "%s timed out, DFU Secs left:  %d)", label, left);
            throw MakeStringException(0, "%s timed out)", label);

        }


        Sleep(polltime*1000);
        polltime *= 2;
        if (polltime>WAIT_SECONDS)
            polltime = WAIT_SECONDS;

    }
}


static void setServerAccess(CClientFileSpray &server, IConstWorkUnit * wu)
{
    StringBuffer user, password, wuid, token;
    wu->getSecurityToken(StringBufferAdaptor(token));
    wu->getWuid(StringBufferAdaptor(wuid));
    extractToken(token.str(), wuid.str(), StringBufferAdaptor(user), StringBufferAdaptor(password));
    server.setUsernameToken(user.str(), password.str(), "");
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, fsfSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    PrintLog("Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientSprayFixed> req = server.createSprayFixedRequest();
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourceRecordSize(recordSize);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);

    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    Owned<IClientSprayFixedResponse> result = server.SprayFixed(req);

    StringBuffer wuid(result->getWuid());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Spray", server, ctx, wuid, timeOut);
    return wuid.detach();
}

static char * implementSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * sourceCsvEscape, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    PrintLog("Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientSprayVariable> req = server.createSprayVariableRequest();
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourceMaxRecordSize(sourceMaxRecordSize);
    req->setSourceFormat(DFUff_csv);
    req->setSourceCsvSeparate(sourceCsvSeparate);
    req->setSourceCsvTerminate(sourceCsvTerminate);
    req->setSourceCsvQuote(sourceCsvQuote);
    if (sourceCsvEscape && *sourceCsvEscape)
        req->setSourceCsvEscape(sourceCsvEscape);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    Owned<IClientSprayResponse> result = server.SprayVariable(req);

    StringBuffer wuid(result->getWuid());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Spray", server, ctx, wuid, timeOut);
    return wuid.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, NULL, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, NULL, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, fsfSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    PrintLog("Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientSprayVariable> req = server.createSprayVariableRequest();
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    DFUfileformat dfufmt;
    if (sourceEncoding == NULL)
        dfufmt = DFUff_utf8;
    else
        dfufmt = CDFUfileformat::decode(sourceEncoding);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourceMaxRecordSize(sourceMaxRecordSize);
    req->setSourceFormat(dfufmt);
    req->setSourceRowTag(sourceRowTag);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    Owned<IClientSprayResponse> result = server.SprayVariable(req);

    StringBuffer wuid(result->getWuid());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Spray", server, ctx, wuid, timeOut);
    return wuid.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsDespray(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite)
{
    CTXFREE(parentCtx, fsfDespray(ctx, sourceLogicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, overwrite));
}

FILESERVICES_API char * FILESERVICES_CALL fsfDespray(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite)
{
    PrintLog("Despray:  %s", sourceLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientDespray> req = server.createDesprayRequest();
    StringBuffer logicalName;
    constructLogicalName(wu, sourceLogicalName, logicalName);

    req->setSourceLogicalName(logicalName.str());
    req->setDestIP(destinationIP);
    req->setDestPath(destinationPath);
    req->setOverwrite(overwrite);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);

    Owned<IClientDesprayResponse> result = server.Despray(req);

    StringBuffer wuid(result->getWuid());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Despray", server, ctx, wuid, timeOut);
    return wuid.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsCopy(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize)
{
    CTXFREE(parentCtx, fsfCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize));
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize)
{
    PrintLog("Copy:  %s%s", sourceLogicalName,asSuperfile?" as superfile":"");

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientCopy> req = server.createCopyRequest();
    if (asSuperfile)
        req->setSuperCopy(true);

    StringBuffer _sourceLogicalName, _destinationLogicalName;
    constructLogicalName(wu, sourceLogicalName, _sourceLogicalName);
    constructLogicalName(wu, destinationLogicalName, _destinationLogicalName);

    req->setSourceLogicalName(_sourceLogicalName.str());
    req->setDestLogicalName(_destinationLogicalName.str());
    if ((destinationGroup != NULL) && (*destinationGroup != '\0'))
        req->setDestGroup(destinationGroup);
    if ((sourceDali != NULL) && (*sourceDali != '\0'))
        req->setSourceDali(sourceDali);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    if (compress)
        req->setCompress(true);
    if (forcePush)
        req->setPush(true);
    if (transferBufferSize > 0)
        req->setTransferBufferSize(transferBufferSize);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);

    Owned<IClientCopyResponse> result = server.Copy(req);

    StringBuffer wuid(result->getResult());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Copy", server, ctx, wuid, timeOut);
    return wuid.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsReplicate(ICodeContext *ctx, const char * sourceLogicalName,int timeOut, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfReplicate(ctx, sourceLogicalName, timeOut, espServerIpPort));
}

FILESERVICES_API char * FILESERVICES_CALL fsfReplicate(ICodeContext *ctx, const char * sourceLogicalName, int timeOut, const char * espServerIpPort)
{
    PrintLog("REPLICATE:  %s", sourceLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);

    Owned<IClientReplicate> req = server.createReplicateRequest();
    StringBuffer logicalName;
    constructLogicalName(wu, sourceLogicalName, logicalName);

    req->setSourceLogicalName(logicalName.str());
    Owned<IClientReplicateResponse> result = server.Replicate(req);

    StringBuffer wuid(result->getWuid());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("Replicate", server, ctx, wuid, timeOut);
    return wuid.detach();

}



//===========================================================================================
// SuperFile API

/*
CreateSuperFile(const varstring lsuperfn, boolean sequentialparts=false);
boolean SuperFileExists(const varstring lsuperfn);
DeleteSuperFile(const varstring lsuperfn,boolean deletesub=false);
unsigned4 GetSuperFileSubCount(const varstring lsuperfn);
varstring GetSuperFileSubName(const varstring lsuperfn,unsigned4 filenum);
unsigned4 FindSuperFileSubName(const varstring lsuperfn,const varstring lfn);
StartSuperFileTransaction();
AddSuperFile(const varstring lsuperfn,const varstring lfn,unsigned4 atpos=0);
RemoveSuperFile(const varstring lsuperfn,const varstring lfn,boolean del=false);
ClearSuperFile(const varstring lsuperfn,boolean del=false);
SwapSuperFile(const varstring lsuperfn1,const varstring lsuperfn2);
ReplaceSuperFile(const varstring lsuperfn,const varstring lfn,const varstring bylfn);
FinishSuperFileTransaction(boolean rollback=false);
*/

static bool lookupSuperFile(ICodeContext *ctx, const char *lsuperfn, Owned<IDistributedSuperFile> &file, bool throwerr, StringBuffer &lsfn, bool allowforeign, bool cacheFiles=false)
{
    lsfn.clear();
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    constructLogicalName(ctx, lsuperfn, lsfn);
    if (!allowforeign) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lsfn.str());
        if (dlfn.isForeign())
            throw MakeStringException(0, "Foreign superfile not allowed: %s", lsfn.str());
    }
    if (cacheFiles)
        file.setown(transaction->lookupSuperFileCached(lsfn.str()));
    else
        file.setown(transaction->lookupSuperFile(lsfn.str()));
    if (file.get())
        return true;
    if (throwerr)
        throw MakeStringException(0, "Could not locate superfile: %s", lsfn.str());
    return false;
}

static ISimpleSuperFileEnquiry *getSimpleSuperFileEnquiry(ICodeContext *ctx, const char *lsuperfn)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active())
        return NULL;
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    return queryDistributedFileDirectory().getSimpleSuperFileEnquiry(lsfn.str(),"Fileservices");
}

static void CheckNotInTransaction(ICodeContext *ctx, const char *fn)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active()) {
        StringBuffer s("Operation not part of transaction : ");
        s.append(fn);
        WUmessage(ctx,ExceptionSeverityWarning,fn,s.str());
    }
}

FILESERVICES_API void FILESERVICES_CALL fsCreateSuperFile(ICodeContext *ctx, const char *lsuperfn, bool sequentialparts, bool ifdoesnotexist)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    Owned<IDistributedSuperFile> file = queryDistributedFileDirectory().createSuperFile(lsfn,udesc,!sequentialparts,ifdoesnotexist,transaction);
    StringBuffer s("CreateSuperFile ('");
    s.append(lsfn).append("') done");
    AuditMessage(ctx,"CreateSuperFile",lsfn.str());
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
}

FILESERVICES_API bool FILESERVICES_CALL fsSuperFileExists(ICodeContext *ctx, const char *lsuperfn)
{
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    return queryDistributedFileDirectory().exists(lsfn,ctx->queryUserDescriptor(),false,true);
}

FILESERVICES_API void FILESERVICES_CALL fsDeleteSuperFile(ICodeContext *ctx, const char *lsuperfn,bool deletesub)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    bool found = lookupSuperFile(ctx, lsuperfn, file, false, lsfn, false);
    file.clear(); // MORE: this should really be exists(file)
    StringBuffer s("DeleteSuperFile ('");
    s.append(lsfn).appendf("')");
    if (found) {
        queryDistributedFileDirectory().removeSuperFile(lsfn.str(), deletesub, udesc, transaction);
        if (transaction->active())
            s.append(" action added to transaction");
        else
            s.append(" done");
    } else {
        s.append(" file not found");
    }
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    if (found)
        AuditMessage(ctx,"DeleteSuperFile",lsfn.str());
}

FILESERVICES_API unsigned FILESERVICES_CALL fsGetSuperFileSubCount(ICodeContext *ctx, const char *lsuperfn)
{
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq)
        return enq->numSubFiles();
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, file, true, lsfn, true);
    return file->numSubFiles();
}

FILESERVICES_API char *  FILESERVICES_CALL fsGetSuperFileSubName(ICodeContext *ctx, const char *lsuperfn,unsigned filenum, bool abspath)
{
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq) {
        if (!filenum||!enq->getSubFileName(filenum-1,ret))
            return CTXSTRDUP(parentCtx, "");
        return ret.detach();
    }
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, file, true, lsfn, true);
    if (!filenum||filenum>file->numSubFiles())
        return CTXSTRDUP(parentCtx, "");
    ret.append(file->querySubFile(filenum-1).queryLogicalName());
    return ret.detach();
}

FILESERVICES_API unsigned FILESERVICES_CALL fsFindSuperFileSubName(ICodeContext *ctx, const char *lsuperfn,const char *_lfn)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq) {
        unsigned n = enq->findSubName(lfn.str());
        return (n==NotFound)?0:n+1;
    }
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, file, true, lsfn, true);
    unsigned n = 0;
    // could do with better version of this TBD
    Owned<IDistributedFileIterator> iter = file->getSubFileIterator();
    ForEach(*iter) {
        n++;
        if (stricmp(iter->query().queryLogicalName(),lfn.str())==0)
            return n;
    }
    return 0;
}

FILESERVICES_API void FILESERVICES_CALL fsStartSuperFileTransaction(IGlobalCodeContext *gctx)
{
    fslStartSuperFileTransaction(gctx->queryCodeContext());
}

FILESERVICES_API void FILESERVICES_CALL fslStartSuperFileTransaction(ICodeContext *ctx)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    transaction->start();
    WUmessage(ctx,ExceptionSeverityInformation,NULL,"StartSuperFileTransaction");
}

FILESERVICES_API void FILESERVICES_CALL fsAddSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *_lfn,unsigned atpos,bool addcontents, bool strict)
{
    fslAddSuperFile(gctx->queryCodeContext(),lsuperfn,_lfn,atpos,addcontents,strict);
}


FILESERVICES_API void FILESERVICES_CALL fslAddSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *_lfn,unsigned atpos,bool addcontents, bool strict)
{
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    // NB: if adding contents, tell lookupSuperFile to cache the subfiles in the transaction
    if (!lookupSuperFile(ctx, lsuperfn, file, strict, lsfn, false, addcontents)) {
        // auto create
        fsCreateSuperFile(ctx,lsuperfn,false,false);
        lookupSuperFile(ctx, lsuperfn, file, true, lsfn, false, addcontents);
    }
    // Never add super file to itself
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    if (stricmp(file->queryLogicalName(), lfn.str()) == 0) {
        throw MakeStringException(0, "AddSuperFile: Adding super file %s to itself!", file->queryLogicalName());
    }
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if  (strict||addcontents) {
        Owned<IDistributedSuperFile> subfile;
        subfile.setown(transaction->lookupSuperFile(lfn.str()));
        if (!subfile.get())
            throw MakeStringException(0, "AddSuperFile%s: Could not locate super file %s", addcontents?"(addcontents)":"",lfn.str());
        if (strict&&(subfile->numSubFiles()<1))
            throw MakeStringException(0, "AddSuperFile: Adding empty super file %s", lfn.str());
    }
    StringBuffer other;
    if (atpos>1)
        other.append("#").append(atpos);
    file->addSubFile(lfn.str(),atpos>0,(atpos>1)?other.str():NULL,addcontents,transaction);
    StringBuffer s("AddSuperFile ('");
    s.append(lsfn).append("', '");
    s.append(lfn).append('\'');
    if (atpos)
        s.append(", ").append(atpos);
    if (addcontents)
        s.append(", addcontents");
    s.append(") ");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"AddSuperFile",lsfn.str(),lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsRemoveSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *_lfn,bool del,bool remcontents)
{
    fslRemoveSuperFile(gctx->queryCodeContext(),lsuperfn,_lfn,del,remcontents);
}

FILESERVICES_API void FILESERVICES_CALL fslRemoveSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *_lfn,bool del,bool remcontents)
{
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    StringBuffer lfn;
    if (_lfn)
        constructLogicalName(ctx, _lfn, lfn);
    lookupSuperFile(ctx, lsuperfn, file, true, lsfn, false, true);
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    file->removeSubFile(_lfn?lfn.str():NULL,del,remcontents,transaction);
    StringBuffer s;
    if (_lfn)
        s.append("RemoveSuperFile ('");
    else
        s.append("ClearSuperFile ('");
    s.append(lsfn).append('\'');
    if (_lfn)
        s.append(", '").append(lfn.str()).append('\'');
    if (del)
        s.append(", del");
    if (remcontents)
        s.append(", remcontents");
    s.append(") ");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"RemoveSuperFile",lsfn.str(),lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsClearSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,bool del)
{
    fsRemoveSuperFile(gctx,lsuperfn,NULL,del);
}

FILESERVICES_API void FILESERVICES_CALL fslClearSuperFile(ICodeContext *ctx, const char *lsuperfn,bool del)
{
    fslRemoveSuperFile(ctx,lsuperfn,NULL,del);
}
FILESERVICES_API void FILESERVICES_CALL fsSwapSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn1,const char *lsuperfn2)
{
    fslSwapSuperFile(gctx->queryCodeContext(),lsuperfn1,lsuperfn2);
}


FILESERVICES_API void FILESERVICES_CALL fslSwapSuperFile(ICodeContext *ctx, const char *lsuperfn1,const char *lsuperfn2)
{
    StringBuffer lsfn1;
    StringBuffer lsfn2;
    Owned<IDistributedSuperFile> file1;
    Owned<IDistributedSuperFile> file2;
    lookupSuperFile(ctx, lsuperfn1, file1, true, lsfn1,false);
    lookupSuperFile(ctx, lsuperfn2, file2, true,lsfn2,false);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    file1->swapSuperFile(file2,transaction);
    StringBuffer s("SwapSuperFile ('");
    s.append(lsfn1).append("', '");
    s.append(lsfn2).append("') '");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"SwapSuperFile",lsfn1.str(),lsfn2.str());
}

FILESERVICES_API void FILESERVICES_CALL fsReplaceSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *lfn,const char *bylfn)
{
    fslReplaceSuperFile(gctx->queryCodeContext(),lsuperfn,lfn,bylfn);
}

FILESERVICES_API void FILESERVICES_CALL fslReplaceSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *lfn,const char *bylfn)
{
    unsigned at = fsFindSuperFileSubName(ctx,lsuperfn,lfn);
    if (!at)
        return;
    fslRemoveSuperFile(ctx,lsuperfn,lfn);
    fslAddSuperFile(ctx,lsuperfn,bylfn,at);
}

FILESERVICES_API void FILESERVICES_CALL fsFinishSuperFileTransaction(IGlobalCodeContext *gctx, bool rollback)
{
    fslFinishSuperFileTransaction(gctx->queryCodeContext(),rollback);
}

FILESERVICES_API void FILESERVICES_CALL fslFinishSuperFileTransaction(ICodeContext *ctx, bool rollback)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active()) {
        if (rollback)
            transaction->rollback();
        else
            transaction->commit();
        StringBuffer s("FinishSuperFileTransaction ");
        if (rollback)
            s.append("rollback");
        else
            s.append("commit");
        WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    }
    else {
        StringBuffer s("Invalid FinishSuperFileTransaction ");
        if (rollback)
            s.append("rollback");
        else
            s.append("done");
        s.append(", transaction not active");
        WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    }
}


FILESERVICES_API char *  FILESERVICES_CALL fsForeignLogicalFileName(ICodeContext *ctx, const char *_lfn,const char *foreigndali,bool abspath)
{
    StringBuffer lfns;
    constructLogicalName(ctx, _lfn, lfns);
    CDfsLogicalFileName lfn;
    lfn.set(lfns.str());
    if (foreigndali&&*foreigndali) {
        SocketEndpoint ep(foreigndali);
        lfn.setForeign(ep,false);
    }
    else
        lfn.clearForeign();
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    lfn.get(ret);
    return ret.detach();
}


FILESERVICES_API char *  FILESERVICES_CALL fsExternalLogicalFileName(const char *location,const char *path,bool abspath)
{
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    CDfsLogicalFileName lfn;
    lfn.setExternal(location,path);
    return lfn.get(ret).detach();
}


FILESERVICES_API char *  FILESERVICES_CALL fsWaitDfuWorkunit(IGlobalCodeContext *gctx, const char *wuid, int timeout, const char * espServerIpPort)
{
    return fslWaitDfuWorkunit(gctx->queryCodeContext(),wuid,timeout,espServerIpPort);
}

FILESERVICES_API char *  FILESERVICES_CALL fslWaitDfuWorkunit(ICodeContext *ctx, const char *wuid, int timeout, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);
    StringBuffer s("Waiting for DFU Workunit ");
    s.append(wuid);
    WUmessage(ctx,ExceptionSeverityInformation,"WaitDfuWorkunit",s.str());
    StringBuffer state;
    wu.clear();
    blockUntilComplete("WaitDfuWorkunit", server, ctx, wuid, timeout, &state);
    s.clear().append("Finished waiting for DFU Workunit ").append(wuid).append(" state=").append(state.str());
    WUmessage(ctx,ExceptionSeverityInformation,"WaitDfuWorkunit",s.str());
    return state.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsAbortDfuWorkunit(IGlobalCodeContext *gctx, const char *wuid, const char * espServerIpPort)
{
    fslAbortDfuWorkunit(gctx->queryCodeContext(),wuid,espServerIpPort);
}

FILESERVICES_API void FILESERVICES_CALL fslAbortDfuWorkunit(ICodeContext *ctx, const char *wuid, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);
    Owned<IClientAbortDFUWorkunit> abortReq = server.createAbortDFUWorkunitRequest();
    abortReq->setWuid(wuid);
    Linked<IClientAbortDFUWorkunitResponse> abortResp = server.AbortDFUWorkunit(abortReq);
    StringBuffer s("DFU Workunit Abort Requested for ");
    s.append(wuid);
    WUmessage(ctx,ExceptionSeverityInformation,"AbortDfuWorkunit",s.str());
}

FILESERVICES_API void  FILESERVICES_CALL fsMonitorLogicalFileName(ICodeContext *ctx, const char *eventname, const char *_lfn,int shotcount, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfMonitorLogicalFileName(ctx, eventname, _lfn,shotcount, espServerIpPort));
}

FILESERVICES_API char *  FILESERVICES_CALL fsfMonitorLogicalFileName(ICodeContext *ctx, const char *eventname, const char *_lfn,int shotcount, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    if (shotcount == 0)
        shotcount = -1;
    Owned<IClientDfuMonitorRequest> req = server.createDfuMonitorRequest();
    req->setEventName(eventname);
    req->setLogicalName(lfn);
    req->setShotLimit(shotcount);
    Owned<IClientDfuMonitorResponse> result = server.DfuMonitor(req);
    StringBuffer res(result->getWuid());
    StringBuffer s("MonitorLogicalFileName ('");
    s.append(lfn).append("'): ").append(res);
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    wu.clear();
    if (res.length()!=0)
        blockUntilComplete("MonitorLogicalFileName",server,ctx,res.str(),1000*60*60,NULL,true);
    return res.detach();
}

FILESERVICES_API void  FILESERVICES_CALL fsMonitorFile(ICodeContext *ctx, const char *eventname,const char *ip, const char *filename, bool sub, int shotcount, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfMonitorFile(ctx, eventname,ip, filename, sub, shotcount, espServerIpPort));
}

FILESERVICES_API char *  FILESERVICES_CALL fsfMonitorFile(ICodeContext *ctx, const char *eventname,const char *ip, const char *filename, bool sub, int shotcount, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getEspServerURL(espServerIpPort));
    setServerAccess(server, wu);
    if (shotcount == 0)
        shotcount = -1;

    Owned<IClientDfuMonitorRequest> req = server.createDfuMonitorRequest();
    req->setEventName(eventname);
    req->setIp(ip);
    req->setFilename(filename);
    req->setShotLimit(shotcount);
    Owned<IClientDfuMonitorResponse> result = server.DfuMonitor(req);
    StringBuffer res(result->getWuid());
    StringBuffer s("MonitorFile (");
    s.append(ip).append(", '").append(filename).append("'): '").append(res);
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    wu.clear();
    if (res.length()!=0)
        blockUntilComplete("MonitorFile",server,ctx,res.str(),1000*60*60,NULL,true);

    return res.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsSetFileDescription(ICodeContext *ctx, const char *logicalfilename, const char *value)
{
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
    if (df) {
        DistributedFilePropertyLock lock(df);
        lock.queryAttributes().setProp("@description",value);
    }
    else
        throw MakeStringException(0, "SetFileDescription: Could not locate file %s", lfn.str());
}

FILESERVICES_API char *  FILESERVICES_CALL fsGetFileDescription(ICodeContext *ctx, const char *logicalfilename)
{
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
    if (!df)
        throw MakeStringException(0, "GetFileDescription: Could not locate file %s", lfn.str());
    const char * ret = df->queryAttributes().queryProp("@description");
    if (ret)
        return CTXSTRDUP(parentCtx, ret);
    else
        return CTXSTRDUP(parentCtx, "");
}

FILESERVICES_API void FILESERVICES_CALL fsRemoteDirectory(size32_t & __lenResult,void * & __result, const char *machine, const char *dir, const char *mask, bool sub)
{
    MemoryBuffer mb;
    RemoteFilename rfn;
    SocketEndpoint ep(machine);
    if (ep.isNull()){
        if (machine)
            throw MakeStringException(-1, "RemoteDirectory: Could not resolve host '%s'", machine);
        ep.setLocalHost(0);
    }

    rfn.setPath(ep,dir);
    Owned<IFile> f = createIFile(rfn);
    if (f) {
        StringBuffer s;
        StringBuffer ds;
        Owned<IDirectoryIterator> di = f->directoryFiles(mask,sub);
        if (di) {
            ForEach(*di) {
                di->getName(s.clear());
                __int64 fsz = di->getFileSize();
                CDateTime dt;
                di->getModifiedTime(dt);
                size32_t sz = s.length();
                dt.getString(ds.clear());
                ds.padTo(19);
                mb.append(sz).append(sz,s.str()).append(fsz).append(19,ds.str());
            }
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsLogicalFileList(ICodeContext *ctx, size32_t & __lenResult,void * & __result, const char *mask, bool includenormal, bool includesuper, bool unknownszero, const char *foreigndali)
{
    MemoryBuffer mb;
    if (!mask||!*mask)
        mask ="*";
    StringBuffer masklower(mask);
    masklower.toLowerCase();
    Owned<IDFAttributesIterator> iter = queryDistributedFileDirectory().getForeignDFAttributesIterator(masklower.str(),ctx->queryUserDescriptor(),true,includesuper,foreigndali);
    if (iter) {
        StringBuffer s;
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            const char *name = attr.queryProp("@name");
            if (!name||!*name)
                continue;
            int numsub = attr.getPropInt("@numsubfiles",-1);
            bool issuper = numsub>=0;
            if (issuper) {
                if (!includesuper)
                    continue;
            }
            else {
                if (!includenormal)
                    continue;
            }
            size32_t sz = strlen(name);
            mb.append(sz).append(sz,name);
            mb.append(issuper);
            __int64 i64;
            __int64 fsz = attr.getPropInt64("@size",-1);
            if ((fsz==-1)&&(unknownszero||(numsub==0)))
                fsz = 0;
            mb.append(fsz);
            i64 = attr.getPropInt64("@recordCount",-1);
            if ((i64==-1)&&(fsz!=-1)) {
                int rsz = attr.getPropInt("@recordSize",0);
                if (rsz>0)
                    i64 = fsz/rsz;
            }
            if ((i64==-1)&&(unknownszero||(numsub==0)))
                i64 = 0;
            mb.append(i64);
            attr.getProp("@modified",s.clear());
            s.padTo(19);
            mb.append(19,s.str());
            attr.getProp("@owner",s.clear());
            sz = s.length();
            mb.append(sz).append(sz,s.str());
            attr.getProp("@group",s.clear());
            sz = s.length();
            mb.append(sz).append(sz,s.str());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();

}

FILESERVICES_API void FILESERVICES_CALL fsSuperFileContents(ICodeContext *ctx, size32_t & __lenResult,void * & __result, const char *lsuperfn, bool recurse)
{
    MemoryBuffer mb;
    Owned<ISimpleSuperFileEnquiry> enq;
    if (!recurse)
        enq.setown(getSimpleSuperFileEnquiry(ctx, lsuperfn));
    if (enq) {
        StringArray subs;
        enq->getContents(subs);
        ForEachItemIn(i,subs) {
            const char *name = subs.item(i);
            size32_t sz = strlen(name);
            if (!sz)
                continue;
            mb.append(sz).append(sz,name);
        }
    }
    else {
        Owned<IDistributedSuperFile> file;
        StringBuffer lsfn;
        lookupSuperFile(ctx, lsuperfn, file, true, lsfn, true);
        Owned<IDistributedFileIterator> iter = file->getSubFileIterator(recurse);
        StringBuffer name;
        ForEach(*iter) {
            iter->getName(name.clear());
            size32_t sz = name.length();
            if (!sz)
                continue;
            mb.append(sz).append(sz,name.str());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsLogicalFileSuperOwners(ICodeContext *ctx,size32_t & __lenResult,void * & __result, const char *logicalfilename)
{
    MemoryBuffer mb;
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);
    StringArray owners;
    if (queryDistributedFileDirectory().getFileSuperOwners(lfn.str(),owners)) {
        ForEachItemIn(i,owners) {
            const char *name = owners.item(i);
            size32_t sz = strlen(name);
            if (!sz)
                continue;
            mb.append(sz).append(sz,name);
        }
    }
    else {
        Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
        if (df) {
            Owned<IDistributedSuperFileIterator> iter = df->getOwningSuperFiles();
            ForEach(*iter) {
                const char *name = iter->queryName();
                size32_t sz = strlen(name);
                if (!sz)
                    continue;
                mb.append(sz).append(sz,name);
            }
        }
        else
            throw MakeStringException(0, "LogicalFileSuperOwners: Could not locate file %s", lfn.str());
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


FILESERVICES_API int  FILESERVICES_CALL fsCompareFiles(ICodeContext *ctx,const char *name1, const char *name2,bool logicalonly, bool usecrcs)
{
    StringBuffer lfn1;
    constructLogicalName(ctx, name1, lfn1);
    StringBuffer lfn2;
    constructLogicalName(ctx, name2, lfn2);
    StringBuffer retstr;
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    int ret = queryDistributedFileDirectory().fileCompare(lfn1.str(),lfn2.str(),usecrcs?DFS_COMPARE_FILES_PHYSICAL_CRCS:(logicalonly?DFS_COMPARE_FILES_LOGICAL:DFS_COMPARE_FILES_PHYSICAL),retstr,udesc);
    if (ret==DFS_COMPARE_RESULT_FAILURE)
        throw MakeStringException(ret,"CompareLogicalFiles: %s",retstr.str());
    return ret;
}

FILESERVICES_API char *  FILESERVICES_CALL fsVerifyFile(ICodeContext *ctx,const char *name,bool usecrcs)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);
    StringBuffer retstr;
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    if (queryDistributedFileDirectory().filePhysicalVerify(lfn.str(),udesc,usecrcs,retstr))
        retstr.append("OK");
    return retstr.detach();
}


// RemotePull

/*
varstring RemotePull(
                      const varstring remoteEspFsURL,                   // remote ESP URL e.g. 'http://10.173.34.60:8010/FileSpray'
                      const varstring sourceLogicalName,                // local
                      const varstring destinationGroup,                 // remote
                      const varstring destinationLogicalName,           // remote (NB full name required)
                      integer4 timeOut=-1,
                      integer4 maxConnections=-1,
                      boolean allowoverwrite=false,
                      boolean replicate=false,
                      boolean asSuperfile=false);
*/


FILESERVICES_API void FILESERVICES_CALL fsRemotePull(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress)
{
    CTXFREE(parentCtx, fsfRemotePull(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress));
}

FILESERVICES_API char * FILESERVICES_CALL fsfRemotePull(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress)
{
    PrintLog("RemotePull(%s):  %s%s", remoteEspFsURL,sourceLogicalName,asSuperfile?" as superfile":"");

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(remoteEspFsURL);
    setServerAccess(server, wu);

    Owned<IClientCopy> req = server.createCopyRequest();
    if (asSuperfile)
        req->setSuperCopy(true);

    StringBuffer _sourceLogicalName, _destinationLogicalName;
    constructLogicalName(wu, sourceLogicalName, _sourceLogicalName);
    // destination name assumed complete (so just skip ~ *)
    while ((*destinationLogicalName=='~')||isspace(*destinationLogicalName))
        destinationLogicalName++;
    _destinationLogicalName.append(destinationLogicalName);
    if (strstr(_destinationLogicalName.str(),"::")==NULL)
        _destinationLogicalName.insert(0,".::");
    StringBuffer _destGroup;
    _destGroup.append(destinationGroup);
    req->setSourceLogicalName(_sourceLogicalName.str());
    req->setDestLogicalName(_destinationLogicalName.str());
    req->setDestGroup(_destGroup.str());
    if (compress)
        req->setCompress(true);
    if (wrap)
        req->setWrap(true);
    StringBuffer sourceDali;
    queryCoven().queryComm().queryGroup().queryNode(0).endpoint().getUrlStr(sourceDali);
    req->setSourceDali(sourceDali);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    if (forcePush)
        req->setPush(true);
    if (transferBufferSize>0)
        req->setTransferBufferSize(transferBufferSize);
    Owned<IClientCopyResponse> result = server.Copy(req);

    StringBuffer wuid(result->getResult());
    if (!wuid.length())
    {
        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringException(0, "%s", errmsg.str());
        }
        else
        {
            throw MakeStringException(0, "Result's dfu WUID is empty");
        }
    }

    wu.clear();

    blockUntilComplete("RemotePull", server, ctx, wuid, timeOut);
    return wuid.detach();
}



FILESERVICES_API void FILESERVICES_CALL fsLogicalFileSuperSubList(ICodeContext *ctx, size32_t & __lenResult,void * & __result)
{
    MemoryBuffer mb;
    getLogicalFileSuperSubList(mb, ctx->queryUserDescriptor());
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API  void FILESERVICES_CALL fsPromoteSuperFileList(ICodeContext * ctx,bool isAllLsuperfns,size32_t  lenLsuperfns,const void * lsuperfns,const char * addhead,bool deltail,bool createonlyonesuperfile,bool reverse)
{
    CTXFREE(parentCtx, fsfPromoteSuperFileList(ctx,isAllLsuperfns,lenLsuperfns,lsuperfns,addhead,deltail,createonlyonesuperfile,reverse));
}

FILESERVICES_API  char * FILESERVICES_CALL fsfPromoteSuperFileList(ICodeContext * ctx,bool isAllLsuperfns,size32_t  lenLsuperfns,const void * lsuperfns,const char * addhead,bool deltail,bool createonlyonesuperfile,bool reverse)
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    MemoryBuffer mb;
    StringBuffer lfn;
    UnsignedArray lfnofs;
    const char *s = (const char *)lsuperfns;
    // MORE - For now, we need a local transaction
    CheckNotInTransaction(ctx, "PromoteSuperFile");

    while ((size32_t)(s-(const char *)lsuperfns)<lenLsuperfns) {
        constructLogicalName(wu,s,lfn.clear());
        lfnofs.append(mb.length());
        mb.append(lfn);
        s = s+strlen(s)+1;
    }
    PointerArray lfns;
    ForEachItemIn(i,lfnofs) {
        lfns.append((void *)(mb.toByteArray()+lfnofs.item(reverse?(lfnofs.ordinality()-i-1):i)));
    }
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringArray toadd;
    toadd.appendListUniq(addhead, ",");
    StringBuffer addlist;
    ForEachItemIn(i1,toadd) {
        if (addlist.length())
            addlist.append(',');
        constructLogicalName(wu,toadd.item(i1),addlist);
    }
    toadd.kill();
    queryDistributedFileDirectory().promoteSuperFiles(lfns.ordinality(),(const char **)lfns.getArray(),addlist.str(),deltail,createonlyonesuperfile,udesc.get(),(unsigned)-1,toadd);
    addlist.clear();
    ForEachItemIn(i2,toadd) {
        if (addlist.length())
            addlist.append(',');
        constructLogicalName(wu,toadd.item(i2),addlist);
    }
    return addlist.detach();
}

FILESERVICES_API unsigned __int64 FILESERVICES_CALL fsGetUniqueInteger(ICodeContext * ctx, const char *foreigndali)
{
    SocketEndpoint ep;
    if (foreigndali&&*foreigndali)
        ep.set(foreigndali);
    IEngineContext *engineContext = ctx->queryEngineContext();
    if (engineContext)
        return engineContext->getGlobalUniqueIds(1,&ep);
    return getGlobalUniqueIds(1,&ep);
}

FILESERVICES_API void FILESERVICES_CALL fsAddFileRelationship(ICodeContext * ctx,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind, const char *cardinality, bool payload, const char *description)
{
    StringBuffer pfn;
    constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    constructLogicalName(ctx, secondary, sfn);
    queryDistributedFileDirectory().addFileRelationship(pfn.str(),sfn.str(),primflds,secflds,kind,cardinality,payload,ctx->queryUserDescriptor(), description);
    StringBuffer s("AddFileRelationship('");
    s.append(pfn.str()).append("','").append(sfn.str()).append("','").append(primflds?primflds:"").append("','").append(secflds?secflds:"").append("','").append(kind?kind:"").append("') done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());

}

static inline void addmbstr(MemoryBuffer &mb,const char *s)
{
    size32_t sz = strlen(s);
    mb.append(sz).append(sz,s);
}

FILESERVICES_API void FILESERVICES_CALL fsFileRelationshipList(ICodeContext * ctx,size32_t & __lenResult,void * & __result,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind)
{
    StringBuffer pfn;
    if (primary&&*primary)
        constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    if (secondary&&*secondary)
        constructLogicalName(ctx, secondary, sfn);
    MemoryBuffer mb;
    Owned<IFileRelationshipIterator> iter = queryDistributedFileDirectory().lookupFileRelationships(pfn.str(),sfn.str(),primflds,secflds,kind);
    if (iter) {
        StringBuffer s;
        ForEach(*iter) {
            IFileRelationship &rel=iter->query();
            addmbstr(mb,rel.queryPrimaryFilename());
            addmbstr(mb,rel.querySecondaryFilename());
            addmbstr(mb,rel.queryPrimaryFields());
            addmbstr(mb,rel.querySecondaryFields());
            addmbstr(mb,rel.queryKind());
            addmbstr(mb,rel.queryCardinality());
            mb.append((byte)(rel.isPayload()?1:0));
            addmbstr(mb,rel.queryDescription());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsRemoveFileRelationship(ICodeContext * ctx,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind)
{
    StringBuffer pfn;
    if (primary&&*primary)
        constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    if (secondary&&*secondary)
        constructLogicalName(ctx, secondary, sfn);
    queryDistributedFileDirectory().removeFileRelationships(pfn.str(),sfn.str(),primflds,secflds,kind);
}

FILESERVICES_API void FILESERVICES_CALL fsSetColumnMapping(ICodeContext * ctx,const char *filename, const char *mapping)
{
    StringBuffer lfn;
    constructLogicalName(ctx, filename, lfn);
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),ctx->queryUserDescriptor(),true);
    if (df)
        df->setColumnMapping(mapping);
    else
        throw MakeStringException(-1, "SetColumnMapping: Could not find logical file %s", lfn.str());
}

FILESERVICES_API char *  FILESERVICES_CALL fsfGetColumnMapping(ICodeContext * ctx,const char *filename)
{
    StringBuffer lfn;
    constructLogicalName(ctx, filename, lfn);
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),ctx->queryUserDescriptor(),true);
    if (df) {
        StringBuffer mapping;
        df->getColumnMapping(mapping);
        return mapping.detach();
    }
    throw MakeStringException(-1, "GetColumnMapping: Could not find logical file %s", lfn.str());
    return NULL;
}


FILESERVICES_API char *  FILESERVICES_CALL fsfRfsQuery(const char *server, const char *query)
{
    StringBuffer ret;
    ret.append('~');
    CDfsLogicalFileName lfn;
    lfn.setQuery(server,query);
    if (!lfn.isSet())
        throw MakeStringException(-1, "RfsQuery invalid parameter");
    return lfn.get(ret).detach();
}

FILESERVICES_API void FILESERVICES_CALL fsRfsAction(const char *server, const char *query)
{
    CDfsLogicalFileName lfn;
    lfn.setQuery(server,query);
    if (!lfn.isSet())
        throw MakeStringException(-1, "RfsAction invalid parameter");
    RemoteFilename rfn;
    lfn.getExternalFilename(rfn);
    Owned<IFile> file = createIFile(rfn);
    Owned<IFileIO> fileio = file->open(IFOread);
    if (fileio) {
        // lets just try reading a byte to cause action
        byte b;
        fileio->read(0,sizeof(b),&b);
    }
}


FILESERVICES_API char *  FILESERVICES_CALL fsfGetHostName(const char *ipaddress)
{
// not a common routine (no Jlib function!) only support IPv4 initially
    StringBuffer ret;
    if (ipaddress&&*ipaddress) {
        IpAddress ip(ipaddress);
        lookupHostName(ip,ret);
    }
    else
        GetHostName(ret);
    return ret.detach();
}



FILESERVICES_API char *  FILESERVICES_CALL fsfResolveHostName(const char *hostname)
{
    StringBuffer ret;
    SocketEndpoint ep(hostname);
    ep.getIpText(ret);
    return ret.detach();
}

static void checkExternalFileRights(ICodeContext *ctx, CDfsLogicalFileName &lfn, bool rd,bool wr)
{
    StringAttr extpath;
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    unsigned auditflags = 0;
    if (rd)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED);
    if (wr)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_WRITE_WANTED);
    int perm = queryDistributedFileDirectory().getFilePermissions(extpath.get(),udesc,auditflags);
    if (wr) {
        if (!HASWRITEPERMISSION(perm)) {
            throw MakeStringException(-1,"Write permission denied for %s",extpath.get());
        }
    }
    else if (rd) {
        if (!HASREADPERMISSION(perm)) {
            throw MakeStringException(-1,"Read permission denied for %s",extpath.get());
        }
    }
}


FILESERVICES_API void  FILESERVICES_CALL fsMoveExternalFile(ICodeContext * ctx,const char *location,const char *frompath,const char *topath)
{
    SocketEndpoint ep(location);
    if (ep.isNull())
        throw MakeStringException(-1,"fsMoveExternalFile: Cannot resolve location %s",location);
    CDfsLogicalFileName from;
    from.setExternal(location,frompath);
    CDfsLogicalFileName to;
    to.setExternal(location,topath);
    checkExternalFileRights(ctx,from,true,true);
    checkExternalFileRights(ctx,to,false,true);
    RemoteFilename fromrfn;
    fromrfn.setPath(ep,frompath);
    RemoteFilename torfn;
    torfn.setPath(ep,topath);
    Owned<IFile> fileto = createIFile(torfn);
    if (fileto->exists())
        throw MakeStringException(-1,"fsMoveExternalFile: Destination %s already exists", topath);
    fileto.clear();
    Owned<IFile> file = createIFile(fromrfn);
    file->move(topath);
    StringBuffer s("MoveExternalFile ('");
    s.append(location).append(',').append(frompath).append(',').append(topath).append(") done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"MoveExternalFile",frompath,topath);
}

FILESERVICES_API void  FILESERVICES_CALL fsDeleteExternalFile(ICodeContext * ctx,const char *location,const char *path)
{
    SocketEndpoint ep(location);
    if (ep.isNull())
        throw MakeStringException(-1,"fsDeleteExternalFile: Cannot resolve location %s",location);
    CDfsLogicalFileName lfn;
    lfn.setExternal(location,path);
    checkExternalFileRights(ctx,lfn,false,true);
    RemoteFilename rfn;
    rfn.setPath(ep,path);
    Owned<IFile> file = createIFile(rfn);
    file->remove();
    StringBuffer s("DeleteExternalFile ('");
    s.append(location).append(',').append(path).append(") done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"DeleteExternalFile",path);
}

FILESERVICES_API void  FILESERVICES_CALL fsCreateExternalDirectory(ICodeContext * ctx,const char *location,const char *path)
{
    SocketEndpoint ep(location);
    if (ep.isNull())
        throw MakeStringException(-1,"fsCreateExternalDirectory: Cannot resolve location %s",location);
    CDfsLogicalFileName lfn;
    lfn.setExternal(location,path);
    checkExternalFileRights(ctx,lfn,false,true);
    RemoteFilename rfn;
    rfn.setPath(ep,path);
    Owned<IFile> file = createIFile(rfn);
    file->createDirectory();
    StringBuffer s("CreateExternalDirectory ('");
    s.append(location).append(',').append(path).append(") done");
    WUmessage(ctx,ExceptionSeverityInformation,NULL,s.str());
    AuditMessage(ctx,"CreateExternalDirectory",path);
}

FILESERVICES_API char * FILESERVICES_CALL fsfGetLogicalFileAttribute(ICodeContext * ctx,const char *_lfn,const char *attrname)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
    StringBuffer ret;
    if (df) {
        if (strcmp(attrname,"ECL")==0)
            df->getECL(ret);
        else if (strcmp(attrname,"clusterName")==0)
            df->getClusterName(0,ret);
        else if (strcmp(attrname,"partmask")==0)
            ret.append(df->queryPartMask());
        else if (strcmp(attrname,"directory")==0)
            ret.append(df->queryDefaultDir());
        else if (strcmp(attrname,"numparts")==0)
            ret.append(df->numParts());
        else if (strcmp(attrname,"name")==0)
            ret.append(df->queryLogicalName());
        else if (strcmp(attrname,"modified")==0) {
            CDateTime dt;
            df->getModificationTime(dt);
            dt.getString(ret);
        }
        else if (strcmp(attrname,"protected")==0) {
            IPropertyTree &attr = df->queryAttributes();
            Owned<IPropertyTreeIterator> piter = attr.getElements("Protect");
            ForEach(*piter) {
                const char *name = piter->get().queryProp("@name");
                if (name&&*name) {
                    unsigned count = piter->get().getPropInt("@count");
                    if (count) {
                        if (ret.length())
                            ret.append(',');
                        ret.append(name);
                    }
                }
            }
        }
        else {
            StringBuffer xpath("@");
            xpath.append(attrname);
            IPropertyTree &attr = df->queryAttributes();
            attr.getProp(xpath.str(),ret);
        }
    }
    else
        throw MakeStringException(0, "GetLogicalFileAttribute: Could not find logical file %s", lfn.str());
    return ret.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsProtectLogicalFile(ICodeContext * ctx,const char *_lfn,bool set)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc);
    StringBuffer ret;
    if (df) {
        StringBuffer u("user:");
        udesc->getUserName(u);
        df->setProtect(u.str(),set);
    }
    else if (set)
        throw MakeStringException(0, "ProtectLogicalFile: Could not find logical file %s", lfn.str());
}

static bool build_dfuplus_globals(int argc, const char *argv[], IProperties * globals)
{
    for (int i = 1; i < argc; i++)
        if (strchr(argv[i],'='))
            globals->loadProp(argv[i]);

    StringBuffer tmp;
    if (globals->hasProp("encrypt")) {
        encrypt(tmp.clear(),globals->queryProp("encrypt") );  // basic encryption at this stage
        globals->setProp("encrypt",tmp.str());
    }
    if (globals->hasProp("decrypt")) {
        encrypt(tmp.clear(),globals->queryProp("decrypt") );  // basic encryption at this stage
        globals->setProp("decrypt",tmp.str());
    }

    return true;
}

FILESERVICES_API void FILESERVICES_CALL fsDfuPlusExec(ICodeContext * ctx,const char *_cmd)
{
    if (!_cmd||!*_cmd)
        return;
    MemoryBuffer mb;
    const char **argv;
    StringBuffer cmdline;
    if (strcmp(_cmd,"dfuplus ")!=0)
        cmdline.append("dfuplus ");
    cmdline.append(_cmd);
    int argc = parseCommandLine(cmdline.str(),mb,argv);

    Owned<IProperties> globals = createProperties(true);

    if (!build_dfuplus_globals(argc, argv, globals))
        throw MakeStringException(-1,"DfuPlusExec: invalid command line");
    const char* server = globals->queryProp("server");
    if (!server || !*server)
        throw MakeStringException(-1,"DfuPlusExec: server url not specified");
    const char* action = globals->queryProp("action");
    if (!action || !*action)
        throw MakeStringException(-1,"DfuPlusExec: no action specified");
    if (ctx) {
        Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
        StringBuffer tmp;
        const char* username = globals->queryProp("username");
        if (!username || !*username)
            globals->setProp("username",udesc->getUserName(tmp.clear()).str());;
        const char* passwd = globals->queryProp("password");
        if (!passwd || !*passwd)
            globals->setProp("password",udesc->getPassword(tmp.clear()).str());;
    }
    class cMsg: implements CDfuPlusMessagerIntercept
    {
        ICodeContext * ctx;
        unsigned limit;
    public:
        cMsg(ICodeContext *_ctx)
        {
            limit = 0;
            ctx = _ctx;
        }

        void info(const char *msg)
        {
            if (ctx&&(++limit<100))
                WUmessage(ctx,ExceptionSeverityInformation,NULL,msg);
        }
        void err(const char *msg)
        {
            throw MakeStringException(-1,"DfuPlusExec: %s",msg);
        }

    } cmsg(ctx);
    try {

        Owned<CDfuPlusHelper> helper = new CDfuPlusHelper(LINK(globals.get()));
        helper->msgintercept = &cmsg;
        helper->doit();
    }
    catch(IException* e) {
        EXCLOG(e,"fsDfuPlusExec");
        throw;
    }
}


