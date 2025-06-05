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

#pragma warning (disable : 4786)

#include "ws_dfuXRefService.hpp"

#include "jconfig.hpp"
#include "dadfs.hpp"
#include "daft.hpp"
#include "dautils.hpp"
#include "wshelpers.hpp"
#include "exception_util.hpp"
#include "package.h"
#include "roxiecontrol.hpp"
#include "hpccconfig.hpp"


static const char* XREF_FEATURE_URL = "DfuXrefAccess";

static void appendReplyMessage(bool json, StringBuffer &reply, const char *href,const char *format,...) __attribute__((format(printf, 4, 5)));
static void appendReplyMessage(bool json, StringBuffer &reply, const char *href,const char *format,...)
{
    va_list args;
    va_start(args, format);
    StringBuffer msg;
    msg.valist_appendf(format, args);
    va_end(args);
    StringBuffer fmsg;
    const char *s=msg.str();
    for (;;) {
        char c=*(s++);
        if (!c||(c=='\n')) {
            Owned<IPropertyTree> tree = createPTree("Message");
            tree->addProp("Value",fmsg.str());
            if (href) {
                tree->addProp("href",href);
                href = NULL;
            }
            if (json)
                toJSON(tree,reply);
            else
                toXML(tree,reply);
            if (!c)
                break;
            fmsg.clear();
        }
        else
            fmsg.append(c);
    }
}

static void dfuXrefXMLToJSON(StringBuffer& buf)
{
    try
    {
        Owned<IPropertyTree> result = createPTreeFromXMLString(buf.str());
        if (result)
            toJSON(result, buf.clear());
        else
            PROGLOG("dfuXrefXMLToJSON() failed in creating PTree.");
    }
    catch (IException *e)
    {
        EXCLOG(e,"dfuXrefXMLToJSON() failed");
        e->Release();
    }
}

const char* CWsDfuXRefEx::formatResult(IEspContext& context, StringBuffer& result, StringBuffer& encodedResult)
{
    if (context.getResponseFormat() == ESPSerializationJSON)
    {
        dfuXrefXMLToJSON(result);
        return result;
    }
    if (context.getClientVersion() < 1.02)
        return result;

    return encodeXML(result, encodedResult);
}

const char* CWsDfuXRefEx::formatResult(IEspContext& context, IPropertyTree* result, StringBuffer& formatedResult)
{
    if (context.getResponseFormat() == ESPSerializationJSON)
        return toJSON(result, formatedResult);

    if (context.getClientVersion() < 1.02)
        return toXML(result, formatedResult);

    StringBuffer tmp;
    toXML(result, tmp);
    return encodeXML(tmp, formatedResult);
}

void CWsDfuXRefEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    DBGLOG("Initializing %s service [process = %s]", service, process);

    if (!daliClientActive())
    {
        OERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }

#ifndef _CONTAINERIZED
    initBareMetalRoxieTargets(roxieConnMap);
#endif

    XRefNodeManager.setown(CreateXRefNodeFactory());

    //Start out builder thread......
    m_XRefbuilder.setown(new CXRefExBuilderThread());
    m_XRefbuilder->start(false);
}

bool CWsDfuXRefEx::onDFUXRefArrayAction(IEspContext &context, IEspDFUXRefArrayActionRequest &req, IEspDFUXRefArrayActionResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Full, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefArrayAction: Permission denied.");

        const char *action = req.getAction();
        const char *type = req.getType();
        if (isEmptyString(action) || isEmptyString(type))
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Action or Type not defined.");
        if (!streq("Attach", action) && !streq("Delete", action) && !streq("DeleteLogical", action))
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Invalid DFUXRefArrayAction: only Attach, Delete or DeleteLogical allowed.");

        StringArray &xrefFiles = req.getXRefFiles();
        if (xrefFiles.ordinality() == 0)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "XRefFile not defined.");

        const char *cluster = req.getCluster();
        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(cluster);
        Owned<IXRefFilesNode> fileNode = getFileNodeInterface(*xRefNode, type);
        if (!fileNode)
        {
            IERRLOG("Unable to find a suitable IXRefFilesNode interface for %s", type);
            throw MakeStringException(ECLWATCH_CANNOT_FIND_IXREFFILESNODE, "Unable to find a suitable IXRefFilesNode interface for %s", type);
        }

        StringBuffer returnStr;
        ESPSerializationFormat fmt = context.getResponseFormat();
        Owned<IUserDescriptor> userDesc = getUserDescriptor(context);
        ForEachItemIn(i, xrefFiles)
        {
            StringBuffer err;
            const char *file = xrefFiles.item(i);
            if (streq("Attach", action))
            {
                if(fileNode->AttachPhysical(file, userDesc, cluster, err))
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Reattached Physical part %s", file);
                else
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Error(s) attaching physical part %s\n%s", file, err.str());
            }
            else if (streq("Delete", action))
            {
                if (fileNode->RemovePhysical(file, userDesc, cluster, err))
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Removed Physical part %s", file);
                else
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Error(s) removing physical part %s\n%s", file, err.str());
            }
            else 
            {   // DeleteLogical:
                // Note we don't want to physically delete 'lost' files - this will end up with orphans on next time round but that is safer
                if (!canRemoveLogicalFile(file, userDesc, req.getRemoveFromSuperfiles(), err))
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Error(s) removing File %s\n%s", file, err.str());
                else if (fileNode->RemoveLogical(file, userDesc, cluster, err))
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Removed Logical File %s", file);
                else
                    appendReplyMessage(fmt==ESPSerializationJSON, returnStr, nullptr,
                        "Error(s) removing File %s\n%s", file, err.str());
            }
        }

        xRefNode->commit();
        if ((fmt == ESPSerializationJSON) || (context.getClientVersion() < 1.02))
            resp.setDFUXRefArrayActionResult(returnStr);
        else
        {
            StringBuffer encodedReturnStr;
            resp.setDFUXRefArrayActionResult(encodeXML(returnStr, encodedReturnStr));
        }
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::canRemoveLogicalFile(const char *logicalFile, IUserDescriptor *userDesc, bool removeFromSuperfiles, StringBuffer &errStr)
{
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalFile, userDesc, AccessMode::tbdWrite, false, false, nullptr, defaultPrivilegedUser);
    if (!df)
    {
        errStr.appendf("Logical file %s not found", logicalFile);
        return false;
    }
    Owned<IDistributedSuperFileIterator> superOwners = df->getOwningSuperFiles();
    if (!superOwners->first())
        return true;

    if (!removeFromSuperfiles)
    {
        errStr.appendf("Cannot remove logical file %s as owned by SuperFile(s)", logicalFile);
        return false;
    }
    return removeLogicalFileFromSuperfiles(logicalFile, superOwners, errStr);
}

bool CWsDfuXRefEx::removeLogicalFileFromSuperfiles(const char *logicalFile, IDistributedSuperFileIterator *superOwners, StringBuffer &errStr)
{
    ForEach(*superOwners)
    {
        IDistributedSuperFile &superOwner = superOwners->query();
        try
        {
            superOwner.removeSubFile(logicalFile, false, false, nullptr);
            PROGLOG("File %s is removed from superfile %s", logicalFile, superOwner.queryLogicalName());
        }
        catch(IException *e)
        {
            errStr.appendf("Could not remove file %s from superfile %s: ", logicalFile, superOwner.queryLogicalName());
            e->errorMessage(errStr);
            e->Release();
            return false;
        }
    }
    return true;
}

IXRefFilesNode *CWsDfuXRefEx::getFileNodeInterface(IXRefNode &XRefNode, const char *nodeType)
{
    if (strieq("Found", nodeType))
        return XRefNode.getFoundFiles();
    else if (strieq("Lost", nodeType))
        return XRefNode.getLostFiles();
    else if (strieq("Orphan", nodeType))
        return XRefNode.getOrphanFiles();

    OWARNLOG("Unrecognized file node type %s", nodeType);
    return nullptr;
}

void CWsDfuXRefEx::readLostFileQueryResult(IEspContext &context, StringBuffer &buf)
{
    Owned<IPropertyTree> lostFilesQueryResult = createPTreeFromXMLString(buf);
    if (!lostFilesQueryResult)
        throw MakeStringException(ECLWATCH_CANNOT_FIND_IXREFFILESNODE, "readLostFileQueryResult() failed in creating PTree.");

    Owned<IUserDescriptor> userDesc = getUserDescriptor(context);
    Owned<IPropertyTreeIterator> iter = lostFilesQueryResult->getElements("File");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        const char *fileName = item.queryProp("Name");
        if (isEmptyString(fileName))
            continue;

        try
        {
            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(fileName, userDesc, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, 0);
            if (df)
                item.addPropInt64("Size", df->getFileSize(true, false));
        }
        catch(IException *e)
        {
            item.addProp("Status", "Warning: this file may be locked now. It can't be recovered as locked.");
            StringBuffer eMsg;
            IERRLOG("Exception in readLostFileQueryResult(): %s", e->errorMessage(eMsg).str());
            e->Release();
        }
    }

    formatResult(context, lostFilesQueryResult, buf.clear());
}

bool CWsDfuXRefEx::onDFUXRefLostFiles(IEspContext &context, IEspDFUXRefLostFilesQueryRequest &req, IEspDFUXRefLostFilesQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefLostFiles: Permission denied.");

        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        Owned<IXRefFilesNode> lostFiles = xRefNode->getLostFiles();
        if (!lostFiles)
            return true;

        StringBuffer buf;
        lostFiles->Serialize(buf);
        if (buf.isEmpty())
            return true;

        readLostFileQueryResult(context, buf);
        resp.setDFUXRefLostFilesQueryResult(buf);
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsDfuXRefEx::onDFUXRefFoundFiles(IEspContext &context, IEspDFUXRefFoundFilesQueryRequest &req, IEspDFUXRefFoundFilesQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefFoundFiles: Permission denied.");

        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        Owned<IXRefFilesNode> foundFiles = xRefNode->getFoundFiles();
        if (!foundFiles)
            return true;

        StringBuffer buf;
        foundFiles->Serialize(buf);
        if (buf.isEmpty())
            return true;

        StringBuffer encodedXML;
        resp.setDFUXRefFoundFilesQueryResult(formatResult(context, buf, encodedXML));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefOrphanFiles(IEspContext &context, IEspDFUXRefOrphanFilesQueryRequest &req, IEspDFUXRefOrphanFilesQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefOrphanFiles: Permission denied.");

        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        Owned<IXRefFilesNode> orphanFiles = xRefNode->getOrphanFiles();
        if (!orphanFiles)
            return true;

        StringBuffer buf;
        orphanFiles->Serialize(buf);
        if (buf.isEmpty())
            return true;

        StringBuffer encodedXML;
        resp.setDFUXRefOrphanFilesQueryResult(formatResult(context, buf, encodedXML));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefMessages(IEspContext &context, IEspDFUXRefMessagesQueryRequest &req, IEspDFUXRefMessagesQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefMessages: Permission denied.");

        StringBuffer buf;
        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        xRefNode->serializeMessages(buf);
        if (buf.isEmpty())
            return true;

        StringBuffer encodedXML;
        resp.setDFUXRefMessagesQueryResult(formatResult(context, buf, encodedXML));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefCleanDirectories(IEspContext &context, IEspDFUXRefCleanDirectoriesRequest &req, IEspDFUXRefCleanDirectoriesResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Write, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefCleanDirectories: Permission denied.");

        StringBuffer err;
        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        xRefNode->removeEmptyDirectories(err);
        if (!err.isEmpty())
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in DFUXRefCleanDirectories: %s", err.str());

        resp.setRedirectUrl(StringBuffer("/WsDFUXRef/DFUXRefDirectories?Cluster=").append(req.getCluster()));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefDirectories(IEspContext &context, IEspDFUXRefDirectoriesQueryRequest &req, IEspDFUXRefDirectoriesQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefDirectories: Permission denied.");

        StringBuffer buf;
        Owned<IXRefNode> xRefNode = getXRefNodeByCluster(req.getCluster());
        xRefNode->serializeDirectories(buf);
        if (buf.isEmpty())
            return true;

        Owned<IPropertyTree> dirs = createPTreeFromXMLString(buf);
        if (!dirs)
            throw MakeStringException(ECLWATCH_INVALID_COMPONENT_INFO,
                "Failed in creating PTree for XRefNode Directories: %s.", req.getCluster());

        Owned<IPropertyTreeIterator> iter = dirs->getElements("Directory");
        ForEach(*iter)
            updateSkew(iter->query());

        resp.setDFUXRefDirectoriesQueryResult(formatResult(context, dirs, buf.clear()));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsDfuXRefEx::updateSkew(IPropertyTree &node)
{
    char *skew = (char*) node.queryProp("Skew");
    if (isEmptyString(skew))
        return;

    StringBuffer positive, negative;
    char *skewPtr = strchr(skew, '/');
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

bool CWsDfuXRefEx::onDFUXRefBuild(IEspContext &context, IEspDFUXRefBuildRequest &req, IEspDFUXRefBuildResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Full, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefBuild: Permission denied.");

        const char *cluster = req.getCluster();
        if (isEmptyString(cluster))
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

        StringBuffer returnStr;
        ESPSerializationFormat fmt = context.getResponseFormat();
        if (m_XRefbuilder->isQueued(cluster))
        { //The XRef build request for this cluster has been queued. No need to queue again.
            appendReplyMessage(fmt == ESPSerializationJSON, returnStr, "/WsDFUXRef/DFUXRefList",
                "An XRef build for cluster %s is in process. Click here to return to the main XRef List.", cluster);
        }
        else
        {
            //create the node if it doesn;t exist
            Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(cluster);
            if (!xRefNode)
                xRefNode.setown(XRefNodeManager->CreateXRefNode(cluster));
            if (!xRefNode)
                throw MakeStringException(ECLWATCH_CANNOT_FIND_IXREFFILESNODE, "XRefNode not created for %s.", cluster);

            if (!m_XRefbuilder->isRunning())
                appendReplyMessage(fmt == ESPSerializationJSON, returnStr, "/WsDFUXRef/DFUXRefList",
                    "Running XRef Process. Click here to return to the main XRef List.");
            else
                appendReplyMessage(fmt == ESPSerializationJSON, returnStr, "/WsDFUXRef/DFUXRefList",
                    "Someone is currently running a Xref build. Your request will be added to the queue. Please click here to return to the main page.");

            m_XRefbuilder->queueRequest(xRefNode, cluster);
        }

        if ((fmt == ESPSerializationJSON) || (context.getClientVersion() < 1.02))
            resp.setDFUXRefActionResult(returnStr);
        else
        {
            StringBuffer encodedReturnStr;
            resp.setDFUXRefActionResult(encodeXML(returnStr, encodedReturnStr));
        }
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfuXRefEx::onDFUXRefBuildCancel(IEspContext &context, IEspDFUXRefBuildCancelRequest &req, IEspDFUXRefBuildCancelResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Full, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefBuildCancel: Permission denied.");

        m_XRefbuilder->cancel();
        StringBuffer returnStr;
        if (context.getResponseFormat() == ESPSerializationJSON)
        {
            returnStr.append("{ \"Message\": { \"Value\": ");
            returnStr.append("\"All Queued items have been cleared. The current running job will continue to execute.\",");
            returnStr.append("\"href\": \"/WsDFUXRef/DFUXRefList\" } }");
            resp.setDFUXRefBuildCancelResult(returnStr);
            return true;
        }

        returnStr.append("<Message><Value>All Queued items have been cleared. The current running job will continue to execute.</Value><href>/WsDFUXRef/DFUXRefList</href></Message>");
        if (context.getClientVersion() < 1.02)
            resp.setDFUXRefBuildCancelResult(returnStr);
        else
        {
            StringBuffer encodedReturnStr;
            resp.setDFUXRefBuildCancelResult(encodeXML(returnStr, encodedReturnStr));
        }
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsDfuXRefEx::addXRefNode(const char* name, IPropertyTree* pXRefNodeTree)
{
    IPropertyTree* XRefTreeNode = pXRefNodeTree->addPropTree("XRefNode", createPTree(ipt_caseInsensitive));
    XRefTreeNode->setProp("Name",name);
    Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(name);
    if (!xRefNode)
    {
        XRefTreeNode->setProp("Modified","");
        XRefTreeNode->setProp("Status","Not Run");
    }
    else
    {
        StringBuffer modified, status;
        XRefTreeNode->setProp("Modified",xRefNode->getLastModified(modified).str());
        XRefTreeNode->setProp("Status",xRefNode->getStatus(status).str());
    }
}

bool CWsDfuXRefEx::onDFUXRefList(IEspContext &context, IEspDFUXRefListRequest &req, IEspDFUXRefListResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(XREF_FEATURE_URL, SecAccess_Read, ECLWATCH_DFU_XREF_ACCESS_DENIED, "WsDfuXRef::DFUXRefList: Permission denied.");

        Owned<IPropertyTreeIterator> planesIter = getPlanesIterator("data", nullptr);
        Owned<IPropertyTree> xrefNodeTree = createPTree("XRefNodes");

        ForEach(*planesIter)
        {
            IPropertyTree &item = planesIter->query();
            bool isNotCopy = !item.getPropBool("@copy", false);
            bool isNotHthorPlane = !item.getPropBool("@hthorplane", false);
            if (isNotCopy && isNotHthorPlane)
                addXRefNode(item.queryProp("@name"), xrefNodeTree);
        }

        if (!isContainerized())
            addXRefNode("SuperFiles", xrefNodeTree);

        StringBuffer buf;
        resp.setDFUXRefListResult(formatResult(context, xrefNodeTree, buf));
    }
    catch(IException *e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

inline void addLfnToUsedFileMap(MapStringTo<bool> &usedFileMap, const char *fileName)
{
    //Normalize file name, including remove the leading tilda.
    CDfsLogicalFileName lfn;
    lfn.set(fileName);
    if (lfn.get())
        usedFileMap.setValue(lfn.get(), true);
}

void addUsedFilesFromPackageMaps(MapStringTo<bool> &usedFileMap, const char *process)
{
    Owned<IPropertyTree> packageSet = resolvePackageSetRegistry(process, true);
    if (!packageSet)
        throw MakeStringException(ECLWATCH_PACKAGEMAP_NOTRESOLVED, "Unable to retrieve package information from dali /PackageMaps");
    StringArray pmids;
#ifdef _CONTAINERIZED
    Owned<IStringIterator> targets = config::getContainerTargets("roxie", process);
#else
    Owned<IStringIterator> targets = getTargetClusters("RoxieCluster", process);
#endif
    ForEach(*targets)
    {
        SCMStringBuffer target;
        VStringBuffer xpath("PackageMap[@querySet='%s']", targets->str(target).str());
        Owned<IPropertyTreeIterator> activeMaps = packageSet->getElements(xpath);
        //Add files referenced in all active maps, for all targets configured for this process cluster
        ForEach(*activeMaps)
        {
            const char *pmid = activeMaps->query().queryProp("@id");
            if (!pmids.appendUniq(pmid))
                continue;
            Owned<IPropertyTree> packageMap = getPackageMapById(pmid, true);
            if (packageMap)
            {
                Owned<IPropertyTreeIterator> subFiles = packageMap->getElements("//SubFile");
                ForEach(*subFiles)
                    addLfnToUsedFileMap(usedFileMap, subFiles->query().queryProp("@value"));
            }
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
        CDfsLogicalFileName lfn;
        lfn.set(files->query().queryProp(nullptr));
        if (lfn.get() && !usedFileMap.getValue(lfn.get()))
            unusedFiles.append(lfn.get());
    }
}

IPropertyTreeIterator *CWsDfuXRefEx::getAllLogicalFilesInCluster(IEspContext &context, const char *cluster, bool &allMatchingFilesReceived)
{
    StringBuffer filterBuf;
    //The filterBuf is sent to dali to retrieve the logical files whose @group attribute contains this cluster.
    WsDFUHelpers::appendDFUQueryFilter(getDFUQFilterFieldName(DFUQFFgroup), DFUQFTcontainString, cluster, ",", filterBuf);

    //If a logical file is for >1 clusters, the localFilterBuf is used to pick up the logical file which is for this cluster.
    StringBuffer localFilterBuf;
    localFilterBuf.append(DFUQFTwildcardMatch).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFgroup)).append(DFUQFilterSeparator).append(cluster);


    DFUQResultField sortOrder[2] = {DFUQResultField::name, DFUQResultField::term};

    __int64 cacheHint = 0; //No paging is needed.
    unsigned totalFiles = 0, pageStart = 0, pageSize = ITERATE_FILTEREDFILES_LIMIT;
    Owned<IUserDescriptor> userDesc = getUserDescriptor(context);

    return queryDistributedFileDirectory().getLogicalFilesSorted(userDesc, sortOrder, filterBuf,
        localFilterBuf, nullptr, pageStart, pageSize, &cacheHint, &totalFiles, &allMatchingFilesReceived);
}

void CWsDfuXRefEx::findUnusedFilesWithDetailsInDFS(IEspContext &context, const char *process, const MapStringTo<bool> &usedFileMap, IArrayOf<IEspDFULogicalFile> &unusedFiles)
{
    //Collect information about logical files in dali for the given cluster.
    bool allMatchingFilesReceived = true;
    Owned<IPropertyTreeIterator> it = getAllLogicalFilesInCluster(context, process, allMatchingFilesReceived);
    if (!it)
        throw MakeStringException(ECLWATCH_CANNOT_GET_FILE_ITERATOR, "Failed to retrieve logical files for %s.", process);

    if (!allMatchingFilesReceived)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "WsDfu::DFURoxieUnusedFiles not supported for %s: too many files.", process);

    //Find out unused Roxie logical files
    double version = context.getClientVersion();
    ForEach(*it)
    {
        IPropertyTree &file = it->query();
        const char *fileName = file.queryProp(getDFUQResultFieldName(DFUQResultField::name));
        if (!isEmptyString(fileName) && !usedFileMap.getValue(fileName))
            WsDFUHelpers::addToLogicalFileList(file, nullptr, version, unusedFiles);
    }
}

void CWsDfuXRefEx::getRoxieFiles(const char *process, bool checkPackageMaps, MapStringTo<bool> &usedFileMap)
{
    SocketEndpointArray servers;
    Owned<IPropertyTree> controlXrefInfo;
#ifdef _CONTAINERIZED
    StringBuffer epStr;
    getService(epStr, process, true);
    SocketEndpoint ep(epStr);
    servers.append(ep);
    if (!servers.length())
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_CLUSTER_INFO, "process cluster, not found.");
    Owned<ISocket> sock = ISocket::connect_timeout(servers.item(0), ROXIECONNECTIONTIMEOUT);
    controlXrefInfo.setown(sendRoxieControlQuery(sock, "<control:getQueryXrefInfo/>", ROXIECONTROLXREFTIMEOUT));
#else
    ISmartSocketFactory *conn = roxieConnMap.getValue(process);
    if (!conn)
        throw makeStringExceptionV(ECLWATCH_CANNOT_GET_ENV_INFO, "Connection info for '%s' process cluster not found.", process ? process : "(null)");

    controlXrefInfo.setown(sendRoxieControlQuery(conn, "<control:getQueryXrefInfo/>", ROXIECONTROLXREFTIMEOUT, ROXIECONNECTIONTIMEOUT));
#endif

    if (!controlXrefInfo)
        throw MakeStringExceptionDirect(ECLWATCH_INTERNAL_ERROR, "roxie cluster, not responding.");
    Owned<IPropertyTreeIterator> roxieFiles = controlXrefInfo->getElements("//File");
    ForEach(*roxieFiles)
        addLfnToUsedFileMap(usedFileMap, roxieFiles->query().queryProp("@name"));

    if (checkPackageMaps)
        addUsedFilesFromPackageMaps(usedFileMap, process);
}

bool CWsDfuXRefEx::onDFUXRefUnusedFiles(IEspContext &context, IEspDFUXRefUnusedFilesRequest &req, IEspDFUXRefUnusedFilesResponse &resp)
{
    const char *process = req.getProcessCluster();
    StringArray &processList = req.getProcessClusterList();
    if (isEmptyString(process) && !processList.length())
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "process cluster not specified.");
    bool checkPackageMaps = req.getCheckPackageMaps();
    MapStringTo<bool> usedFileMap;
    if (!isEmptyString(process))
        getRoxieFiles(process, checkPackageMaps, usedFileMap);
    ForEachItemIn(i, processList)
    {
        getRoxieFiles(processList.item(i), checkPackageMaps, usedFileMap);
    }

    StringArray &checkPlanes = req.getCheckPlanes();
    if (isContainerized() && (checkPlanes.length() == 0))
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Storage planes must be specified for a containerized system");

    if (!req.getGetFileDetails())
    {
        StringArray unusedFiles;
        if (checkPlanes.length())
        {
            ForEachItemIn(idx, checkPlanes)
            {
                findUnusedFilesInDFS(unusedFiles, checkPlanes.item(idx), usedFileMap);

            }
        }
        else
            findUnusedFilesInDFS(unusedFiles, process, usedFileMap);
        resp.setUnusedFileCount(unusedFiles.length());
        resp.setUnusedFiles(unusedFiles);
    }
    else
    {
        IArrayOf<IEspDFULogicalFile> unusedLFs;
        if (checkPlanes.length())
        {
            ForEachItemIn(idx, checkPlanes)
            {
                findUnusedFilesWithDetailsInDFS(context, checkPlanes.item(idx), usedFileMap, unusedLFs);
            }
        }
        else
            findUnusedFilesWithDetailsInDFS(context, process, usedFileMap, unusedLFs);
        resp.setUnusedFileCount(unusedLFs.length());
        resp.setUnusedFilesWithDetails(unusedLFs);
    }
    return true;
}

IXRefNode *CWsDfuXRefEx::getXRefNodeByCluster(const char* cluster)
{
    if (isEmptyString(cluster))
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Cluster not defined.");

    Owned<IXRefNode> xRefNode = XRefNodeManager->getXRefNode(cluster);
    if (!xRefNode)
        throw MakeStringException(ECLWATCH_CANNOT_FIND_IXREFFILESNODE, "XRefNode not found for %s.", cluster);

    return xRefNode.getClear();
}

IUserDescriptor *CWsDfuXRefEx::getUserDescriptor(IEspContext &context)
{
    StringBuffer userName;
    context.getUserID(userName);
    if (userName.isEmpty())
        return nullptr;

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(userName, context.queryPassword(), context.querySignature());
    return userDesc.getClear();
}
