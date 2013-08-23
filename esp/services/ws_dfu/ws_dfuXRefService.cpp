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

#include "ws_dfuXRefService.hpp"

#include "dadfs.hpp"
#include "wshelpers.hpp"
#include "LogicFileWrapper.hpp"
#include "exception_util.hpp"
#include "package.h"
#include "roxiecontrol.hpp"


static const char* FEATURE_URL = "DfuXrefAccess";

static void appendReplyMessage(StringBuffer &reply, const char *href,const char *format,...) __attribute__((format(printf, 3, 4)));
static void appendReplyMessage(StringBuffer &reply, const char *href,const char *format,...) 
{
    va_list args;
    va_start(args, format);
    StringBuffer msg;
    msg.valist_appendf(format, args);
    va_end(args);
    StringBuffer fmsg;
    const char *s=msg.str();
    loop {
        char c=*(s++);
        if (!c||(c=='\n')) {
            Owned<IPropertyTree> tree = createPTree("Message");
            tree->addProp("Value",fmsg.str());
            if (href) {
                tree->addProp("href",href);
                href = NULL;
            }
            toXML(tree,reply);
            if (!c)
                break;
            fmsg.clear();
        }
        else
            fmsg.append(c);
    }
}

void CWsDfuXRefEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    
    StringBuffer xpath;
    
    DBGLOG("Initializing %s service [process = %s]", service, process);
    
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/User", process, service);
    cfg->getProp(xpath.str(), user_);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/Password", process, service);
    cfg->getProp(xpath.str(), password_);

    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }
    XRefNodeManager.setown(CreateXRefNodeFactory());    

    //Start out builder thread......
    m_XRefbuilder.setown(new CXRefExBuilderThread());
    m_XRefbuilder->start();
}

bool CWsDfuXRefEx::onDFUXRefArrayAction(IEspContext &context, IEspDFUXRefArrayActionRequest &req, IEspDFUXRefArrayActionResponse &resp)
{
    try
    {
        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefArrayAction User=%s",username.str());
        Owned<IUserDescriptor> userdesc;
        if(username.length() > 0)
        {
            const char* passwd = context.queryPassword();
            userdesc.setown(createUserDescriptor());
            userdesc->set(username.str(), passwd);
        }

        if(*req.getAction() == 0 || *req.getType() == 0 || *req.getCluster() == 0)
        {
            ERRLOG("Invalid Parameters into CWsDfuXRefEx::onDFUXRefArrayAction");
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Action, cluster, or type not defined.");
        }
        
        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
        {
            ERRLOG("Unable to resolve XRef cluster name %s",req.getCluster());
            throw MakeStringException(ECLWATCH_CANNOT_RESOLVE_CLUSTER_NAME, "Unable to resolve cluster name %s",req.getCluster());
        }

        
        Owned<IXRefFilesNode> _fileNode = getFileNodeInterface(*xRefNode.get(),req.getType());
        if (_fileNode.get() == 0)
        {
            ERRLOG("Unable to find a suitable IXRefFilesNode interface for %s",req.getType());
            throw MakeStringException(ECLWATCH_CANNOT_FIND_IXREFFILESNODE, "Unable to find a suitable IXRefFilesNode interface for %s",req.getType());
        }

        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to run Xref command. Permission denied.");
       
        StringBuffer returnStr,UserName;
        const char* ActionType = req.getAction();

        DBGLOG("Running Xref Command %s for user %s",ActionType,context.getUserID(UserName).str());

        for(unsigned i = 0; i < req.getXRefFiles().length();i++)
        {
            DBGLOG("CWsDfuXRefEx::onDFUXRefArrayAction %s file %s for User=%s", ActionType, req.getXRefFiles().item(i), username.str());

            StringBuffer errstr;
            if (strcmp("Delete" ,ActionType) == 0)
            {
                if (_fileNode->RemovePhysical(req.getXRefFiles().item(i),userdesc,req.getCluster(),errstr))
                    appendReplyMessage(returnStr,NULL,"Removed Physical part %s",req.getXRefFiles().item(i));
                else
                    appendReplyMessage(returnStr,NULL,"Error(s) removing physical part %s\n%s",req.getXRefFiles().item(i),errstr.str());
            }
            else if (strcmp("Attach" ,ActionType) == 0)
            {
                if(_fileNode->AttachPhysical(req.getXRefFiles().item(i),userdesc,req.getCluster(),errstr) )
                    appendReplyMessage(returnStr,NULL,"Reattached Physical part %s",req.getXRefFiles().item(i));
                else
                    appendReplyMessage(returnStr,NULL,"Error(s) attaching physical part %s\n%s",req.getXRefFiles().item(i),errstr.str());
            }
            if (strcmp("DeleteLogical" ,ActionType) == 0)
            {
                // Note we don't want to physically delete 'lost' files - this will end up with orphans on next time round but that is safer
                if (_fileNode->RemoveLogical(req.getXRefFiles().item(i),userdesc,req.getCluster(),errstr)) {
                    appendReplyMessage(returnStr,NULL,"Removed Logical File %s",req.getXRefFiles().item(i));
                }
                else
                    appendReplyMessage(returnStr,NULL,"Error(s) removing File %s\n%s",req.getXRefFiles().item(i),errstr.str());
            }
        }

        xRefNode->commit();
        resp.setDFUXRefArrayActionResult(returnStr.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IXRefFilesNode* CWsDfuXRefEx::getFileNodeInterface(IXRefNode& XRefNode,const char* nodeType)
{
    if (strcmp("Found" ,nodeType) == 0)
        return XRefNode.getFoundFiles();
    else if (strcmp("Lost" ,nodeType) == 0)
        return XRefNode.getLostFiles();
    else if (strcmp("Orphan" ,nodeType) == 0)
        return XRefNode.getOrphanFiles();
    else
        WARNLOG("Unrecognized file node type %s",nodeType);
    return 0;
}

bool CWsDfuXRefEx::onDFUXRefLostFiles(IEspContext &context, IEspDFUXRefLostFilesQueryRequest &req, IEspDFUXRefLostFilesQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to read Xref Lost Files. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefLostFiles User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;

      Owned<IXRefFilesNode> _lost = xRefNode->getLostFiles();
      StringBuffer buf;
      resp.setDFUXRefLostFilesQueryResult(_lost->Serialize(buf).str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsDfuXRefEx::onDFUXRefFoundFiles(IEspContext &context, IEspDFUXRefFoundFilesQueryRequest &req, IEspDFUXRefFoundFilesQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to read Xref Found Files. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefFoundFiles User=%s",username.str());

        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;
        Owned<IXRefFilesNode> _found = xRefNode->getFoundFiles();
        StringBuffer buf;
        resp.setDFUXRefFoundFilesQueryResult(_found->Serialize(buf).str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefOrphanFiles(IEspContext &context, IEspDFUXRefOrphanFilesQueryRequest &req, IEspDFUXRefOrphanFilesQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to read Xref Orphan Files. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefOrphanFiles User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;

        Owned<IXRefFilesNode> _orphan = xRefNode->getOrphanFiles();
        StringBuffer buf;
        _orphan->Serialize(buf);
        resp.setDFUXRefOrphanFilesQueryResult(buf.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefMessages(IEspContext &context, IEspDFUXRefMessagesQueryRequest &req, IEspDFUXRefMessagesQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to get Xref Messages. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefMessages User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;

        StringBuffer buf;
        xRefNode->serializeMessages(buf);
        resp.setDFUXRefMessagesQueryResult(buf.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefCleanDirectories(IEspContext &context, IEspDFUXRefCleanDirectoriesRequest &req, IEspDFUXRefCleanDirectoriesResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to clean Xref Directories. Permission denied.");
        
        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefDirectories User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;

        StringBuffer buf;
        xRefNode->removeEmptyDirectories(buf);
        DBGLOG("xRefNode->removeEmptyDirectories result=%s",buf.str());
        resp.setRedirectUrl(StringBuffer("/WsDFUXRef/DFUXRefDirectories?Cluster=").append(req.getCluster()));
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefDirectories(IEspContext &context, IEspDFUXRefDirectoriesQueryRequest &req, IEspDFUXRefDirectoriesQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to get Xref Directories. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefDirectories User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
            return false;

        StringBuffer buf0;
        xRefNode->serializeDirectories(buf0);

        Owned <IPropertyTree> dirs = createPTreeFromXMLString(buf0.str()); // Why are we doing this?
        Owned<IPropertyTreeIterator> iter = dirs->getElements("Directory");
        ForEach(*iter)
        {
            IPropertyTree &node = iter->query();

            StringBuffer positive, negative;
            char* skew = (char*) node.queryProp("Skew");
            if (!skew || !*skew)
                continue;

            char* skewPtr = strchr(skew, '/');
            if (skewPtr)
            {
                if (skew[0] == '+' && (strlen(skew) > 1))
                    positive.append(skewPtr - skew - 1, skew+1);
                else
                    positive.append(skewPtr - skew, skew);
                skewPtr++;
                if (skewPtr)
                {
                    if (skewPtr[0] == '-')
                        negative.append(skewPtr+1);
                    else
                        negative.append(skewPtr);
                }
            }
            else
            {
                if (skew[0] == '+' && (strlen(skew) > 1))
                    positive.append(skew+1);
                else
                    positive.append(skew);
            }

            node.removeProp("Skew");
            node.addProp("PositiveSkew", positive);
            node.addProp("NegativeSkew", negative);
        }
        
        StringBuffer buf;
        toXML(dirs, buf);
        resp.setDFUXRefDirectoriesQueryResult(buf.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefBuild(IEspContext &context, IEspDFUXRefBuildRequest &req, IEspDFUXRefBuildResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to build Xref. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefBuild User=%s",username.str());
        if (!req.getCluster() || !*req.getCluster())
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        //create the node if it doesn;t exist
        Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(req.getCluster());
        if (xRefNode.get() == 0)
        {
            xRefNode.setown( XRefNodeManager->CreateXRefNode(req.getCluster()));
        }
        StringBuffer returnStr;
        if (m_XRefbuilder->IsQueued(req.getCluster()) )
            appendReplyMessage(returnStr,"/WsDFUXRef/DFUXRefList","An XRef build for cluster %s is in process. Click here to return to the main XRef List.",req.getCluster());
        else if (!m_XRefbuilder->IsRunning())
            appendReplyMessage(returnStr,"/WsDFUXRef/DFUXRefList","Running XRef Process. Click here to return to the main XRef List.");
        else
            appendReplyMessage(returnStr,"/WsDFUXRef/DFUXRefList","someone is currently running a Xref build. Your request will be added to the queue. Please click here to return to the main page.");


        m_XRefbuilder->QueueRequest(xRefNode,req.getCluster());
        resp.setDFUXRefActionResult(returnStr.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefBuildCancel(IEspContext &context, IEspDFUXRefBuildCancelRequest &req, IEspDFUXRefBuildCancelResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to cancel Xref Build. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefBuildCancel User=%s",username.str());

        m_XRefbuilder->Cancel();
        StringBuffer returnStr;
        returnStr.appendf("<Message><Value>All Queued items have been cleared. The current running job will continue to execute.</Value><href>/WsDFUXRef/DFUXRefList</href></Message>");    
        resp.setDFUXRefBuildCancelResult(returnStr.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefList(IEspContext &context, IEspDFUXRefListRequest &req, IEspDFUXRefListResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_XREF_ACCESS_DENIED, "Failed to access Xref. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsDfuXRefEx::onDFUXRefList User=%s",username.str());


        //Firstly we need to get a list of the available Thor Cluster....
        IArrayOf<IEspTpCluster> clusters;
        CTpWrapper _topology;
        _topology.getClusterProcessList(eqThorCluster,clusters,false,true);
        ///_topology.getClusterList(eqRoxieCluster,clusters,false,true);

        Owned<IPropertyTree> pXRefNodeTree = createPTree("XRefNodes");
        //DBGLOG("CWsDfuXRefEx::onDFUXRefList1\n");

        for (unsigned x=0;x<=clusters.ordinality();x++)
        {
            IPropertyTree* XRefTreeNode = pXRefNodeTree->addPropTree("XRefNode", createPTree(ipt_caseInsensitive));
            
            IEspTpCluster* cluster = x<clusters.ordinality()?&clusters.item(x):NULL;        
            const char *clustername = cluster?cluster->getName():"SuperFiles";

            XRefTreeNode->setProp("Name",clustername);
            //create the node if it doesn;t exist
            Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(clustername);
            if (xRefNode == 0)
            {
                XRefTreeNode->setProp("Modified","");
                XRefTreeNode->setProp("Status","Not Run");
            }
            else
            {
                  StringBuffer buf;
                XRefTreeNode->setProp("Modified",xRefNode->getLastModified(buf).str());
                    buf.clear();
                XRefTreeNode->setProp("Status",xRefNode->getStatus(buf).str());
            }
        }
        

        StringBuffer buf;
        resp.setDFUXRefListResult(toXML(pXRefNodeTree, buf).str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

inline const char *skipTilda(const char *lfn) //just in case
{
    if (lfn)
        while (*lfn == '~' || *lfn == ' ')
            lfn++;
    return lfn;
}

inline void addLfnToUsedFileMap(MapStringTo<bool> &usedFileMap, const char *lfn)
{
    lfn = skipTilda(lfn);
    if (lfn)
        usedFileMap.setValue(lfn, true);
}

void addUsedFilesFromActivePackageMaps(MapStringTo<bool> &usedFileMap, const char *process)
{
    Owned<IPropertyTree> packageSet = resolvePackageSetRegistry(process, true);
    if (!packageSet)
        throw MakeStringException(ECLWATCH_PACKAGEMAP_NOTRESOLVED, "Unable to retrieve package information from dali /PackageMaps");
    Owned<IPropertyTreeIterator> activeMaps = packageSet->getElements("PackageMap[@active='1']");
    //Add files referenced in all active maps, for all targets configured for this process cluster
    ForEach(*activeMaps)
    {
        Owned<IPropertyTree> packageMap = getPackageMapById(activeMaps->query().queryProp("@id"), true);
        if (packageMap)
        {
            Owned<IPropertyTreeIterator> subFiles = packageMap->getElements("//SubFile");
            ForEach(*subFiles)
                addLfnToUsedFileMap(usedFileMap, subFiles->query().queryProp("@value"));
        }
    }
}

void findUnusedFilesInDFS(StringArray &unusedFiles, const char *process, const MapStringTo<bool> &usedFileMap)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/Files/", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    Owned<IPropertyTree> root = globalLock->getRoot();

    VStringBuffer xpath("//File[Cluster/@name='%s']/OrigName", process);
    Owned<IPropertyTreeIterator> files = root->getElements(xpath);
    ForEach(*files)
    {
        const char *lfn = skipTilda(files->query().queryProp(NULL));
        if (lfn && !usedFileMap.getValue(lfn))
            unusedFiles.append(lfn);
    }
}
bool CWsDfuXRefEx::onDFUXRefUnusedFiles(IEspContext &context, IEspDFUXRefUnusedFilesRequest &req, IEspDFUXRefUnusedFilesResponse &resp)
{
    const char *process = req.getProcessCluster();
    if (!process || !*process)
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "process cluster, not specified.");

    SocketEndpointArray servers;
    getRoxieProcessServers(process, servers);
    if (!servers.length())
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_CLUSTER_INFO, "process cluster, not found.");

    Owned<ISocket> sock = ISocket::connect_timeout(servers.item(0), 5000);
    Owned<IPropertyTree> controlXrefInfo = sendRoxieControlQuery(sock, "<control:getQueryXrefInfo/>", 5000);
    if (!controlXrefInfo)
        throw MakeStringExceptionDirect(ECLWATCH_INTERNAL_ERROR, "roxie cluster, not responding.");
    MapStringTo<bool> usedFileMap;
    Owned<IPropertyTreeIterator> roxieFiles = controlXrefInfo->getElements("//File");
    ForEach(*roxieFiles)
        addLfnToUsedFileMap(usedFileMap, roxieFiles->query().queryProp("@name"));
    if (req.getCheckPackageMaps())
        addUsedFilesFromActivePackageMaps(usedFileMap, process);
    StringArray unusedFiles;
    findUnusedFilesInDFS(unusedFiles, process, usedFileMap);
    resp.setUnusedFileCount(unusedFiles.length());
    resp.setUnusedFiles(unusedFiles);
    return true;
}
