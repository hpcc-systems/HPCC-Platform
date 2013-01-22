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

#include "ws_packageprocessService.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dfuutil.hpp"
#include "ws_fs.hpp"
#include "ws_workunits.hpp"
#include "packageprocess_errors.h"
#include "referencedfilelist.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

void CWsPackageProcessEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
}

bool CWsPackageProcessEx::onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp)
{
    StringBuffer respMsg;
    ISecUser* user = context.queryUser();
    if(user != NULL)
    {
        const char* name = user->getName();
        if (name && *name)
            respMsg.appendf("%s: ", name);
    }

    const char* reqMsg = req.getRequest();
    if (reqMsg && *reqMsg)
        respMsg.append(reqMsg);
    else
        respMsg.append("??");

    resp.setResponse(respMsg.str());
    return true;
}

IPropertyTree *getPkgSetRegistry(const char *id, const char *process, bool readonly)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(id).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        if (readonly)
            return NULL;
        Owned<IPropertyTree> querySet = createPTree();
        querySet->setProp("@id", id);
        if (!process || !*process)
            querySet->setProp("@process", "*");
        else
            querySet->setProp("@process", process);
        globalLock->queryRoot()->addPropTree("PackageSet", querySet.getClear());
        globalLock->commit();

        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!conn)
            throw MakeStringException(PKG_DALI_LOOKUP_ERROR, "Unable to retrieve package information from dali %s", xpath.str());
    }

    return conn->getRoot();
}

////////////////////////////////////////////////////////////////////////////////////////
const unsigned roxieQueryRoxieTimeOut = 60000;

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

bool isRoxieProcess(const char *process)
{
    if (!process)
        return false;
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return false;
    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", process);
    return conn->queryRoot()->hasProp(xpath.str());
}

bool isFileKnownOnCluster(const char *logicalname, const char *lookupDaliIp, IConstWUClusterInfo *clusterInfo, IUserDescriptor* userdesc)
{
    Owned<IDistributedFile> dst = queryDistributedFileDirectory().lookup(logicalname, userdesc, true);
    if (dst)
    {
        SCMStringBuffer processName;
        clusterInfo->getRoxieProcess(processName);
        if (dst->findCluster(processName.str()) != NotFound)
            return true; // file already known for this cluster
    }
    return false;
}

bool isFileKnownOnCluster(const char *logicalname, const char *lookupDaliIp, const char *target, IUserDescriptor* userdesc)
{
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
    if (!clusterInfo)
        throw MakeStringException(PKG_TARGET_NOT_DEFINED, "Could not find information about target cluster %s ", target);

    return isFileKnownOnCluster(logicalname, lookupDaliIp, clusterInfo, userdesc);
}

bool cloneFileInfoToDali(StringArray &fileNames, const char *lookupDaliIp, IConstWUClusterInfo *clusterInfo, bool overWrite, IUserDescriptor* userdesc)
{
    bool retval = true;
    try
    {
        StringBuffer user;
        StringBuffer password;

        if (userdesc)
        {
            userdesc->getUserName(user);
            userdesc->getPassword(password);
        }

        Owned<IReferencedFileList> wufiles = createReferencedFileList(user, password);
        wufiles->addFiles(fileNames);
        SCMStringBuffer processName;
        clusterInfo->getRoxieProcess(processName);
        wufiles->resolveFiles(processName.str(), lookupDaliIp, !overWrite, false);
        wufiles->cloneAllInfo(overWrite, true);
    }
    catch(IException *e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        DBGLOG("ERROR = %s", msg.str());
        e->Release();  // report the error later if needed
        retval = false;
    }
    catch(...)
    {
        retval = false;
    }

    return retval;
}

bool cloneFileInfoToDali(StringArray &fileNames, const char *lookupDaliIp, const char *target, bool overWrite, IUserDescriptor* userdesc)
{
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
    if (!clusterInfo)
        throw MakeStringException(PKG_TARGET_NOT_DEFINED, "Could not find information about target cluster %s ", target);

    return cloneFileInfoToDali(fileNames, lookupDaliIp, clusterInfo, overWrite, userdesc);
}


void makePackageActive(IPropertyTree *pkgSetRegistry, IPropertyTree *pkgSetTree, const char *setName)
{
    VStringBuffer xpath("PackageMap[@querySet='%s'][@active='1']", setName);
    Owned<IPropertyTreeIterator> iter = pkgSetRegistry->getElements(xpath.str());
    ForEach(*iter)
    {
        iter->query().setPropBool("@active", false);
    }
    pkgSetTree->setPropBool("@active", true);
}

//////////////////////////////////////////////////////////

void addPackageMapInfo(IPropertyTree *pkgSetRegistry, const char *target, const char *packageMapName, const char *packageSetName, const char *lookupDaliIp, IPropertyTree *packageInfo, bool active, bool overWrite, IUserDescriptor* userdesc)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    StringBuffer lcName(packageMapName);
    lcName.toLowerCase();
    StringBuffer xpath;
    xpath.append("PackageMap[@id='").append(lcName).append("']");

    IPropertyTree *pkgRegTree = pkgSetRegistry->queryPropTree(xpath.str());
    IPropertyTree *root = globalLock->queryRoot();
    IPropertyTree *mapTree = root->queryPropTree(xpath);

    if (!overWrite && (pkgRegTree || mapTree))
        throw MakeStringException(PKG_NAME_EXISTS, "Package name %s already exists, either delete it or specify overwrite", lcName.str());

    if (mapTree)
        root->removeTree(mapTree);

    if (pkgRegTree)
        pkgSetRegistry->removeTree(pkgRegTree);


    mapTree = root->addPropTree("PackageMap", createPTree());
    mapTree->addProp("@id", packageMapName);

    StringArray fileNames;
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
    if (!clusterInfo)
        throw MakeStringException(PKG_TARGET_NOT_DEFINED, "Could not find information about target cluster %s ", target);

    IPropertyTree *baseInfo = createPTree();
    Owned<IPropertyTreeIterator> iter = packageInfo->getElements("Package");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        Owned<IPropertyTreeIterator> super_iter = item.getElements("SuperFile");
        if (super_iter->first())
        {
            ForEach(*super_iter)
            {
                IPropertyTree &supertree = super_iter->query();
                StringBuffer lc(supertree.queryProp("@id"));
                const char *id = lc.toLowerCase().str();
                if (*id == '~')
                    id++;
                supertree.setProp("@id", id);

                Owned<IPropertyTreeIterator> sub_iter = supertree.getElements("SubFile");
                ForEach(*sub_iter)
                {
                    IPropertyTree &subtree = sub_iter->query();
                    StringAttr subid = subtree.queryProp("@value");
                    if (subid.length())
                    {
                        if (subid[0] == '~')
                            subtree.setProp("@value", subid+1);
                        if (!isFileKnownOnCluster(subid, lookupDaliIp, clusterInfo, userdesc))
                            fileNames.append(subid);
                    }
                }
                mapTree->addPropTree("Package", LINK(&item));
            }
        }
        else
        {
            baseInfo->addPropTree("Package", LINK(&item));
        }
    }

    mergePTree(mapTree, baseInfo);
    cloneFileInfoToDali(fileNames, lookupDaliIp, clusterInfo, overWrite, userdesc);

    globalLock->commit();

    IPropertyTree *pkgSetTree = pkgSetRegistry->addPropTree("PackageMap", createPTree("PackageMap"));
    pkgSetTree->setProp("@id", lcName);
    pkgSetTree->setProp("@querySet", target);
    if (active)
        makePackageActive(pkgSetRegistry, pkgSetTree, target);
    else
        pkgSetTree->setPropBool("@active", false);
}

void getPackageListInfo(IPropertyTree *mapTree, IEspPackageListMapData *pkgList)
{
    pkgList->setId(mapTree->queryProp("@id"));
    pkgList->setTarget(mapTree->queryProp("@querySet"));

    Owned<IPropertyTreeIterator> iter = mapTree->getElements("Package");
    IArrayOf<IConstPackageListData> results;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();

        Owned<IEspPackageListData> res = createPackageListData("", "");
        res->setId(item.queryProp("@id"));
        if (item.hasProp("@queries"))
            res->setQueries(item.queryProp("@queries"));
        results.append(*res.getClear());
    }
    pkgList->setPkgListData(results);
}
void getAllPackageListInfo(IPropertyTree *mapTree, StringBuffer &info)
{
    info.append("<PackageMap id='").append(mapTree->queryProp("@id")).append("'");

    Owned<IPropertyTreeIterator> iter = mapTree->getElements("Package");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        info.append("<Package id='").append(item.queryProp("@id")).append("'");
        if (item.hasProp("@queries"))
            info.append(" queries='").append(item.queryProp("@queries")).append("'");
        info.append("></Package>");
    }
    info.append("</PackageMap>");
}
void listPkgInfo(const char *target, const char *process, IArrayOf<IConstPackageListMapData>* results)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        throw MakeStringException(PKG_DALI_LOOKUP_ERROR, "Unable to retrieve package information from dali /PackageMaps");
    IPropertyTree *root = globalLock->queryRoot();
    Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry((process && *process) ? process : "*", NULL, true);
    if (!pkgSetRegistry)
        throw MakeStringException(PKG_DALI_LOOKUP_ERROR, "Unable to retrieve package information from dali for process %s", (process && *process) ? process : "*");

    StringBuffer xpath("PackageMap");
    if (target && *target)
        xpath.appendf("[@querySet='%s']", target);
    Owned<IPropertyTreeIterator> iter = pkgSetRegistry->getElements(xpath.str());
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        const char *id = item.queryProp("@id");
        if (id)
        {
            StringBuffer xpath;
            xpath.append("PackageMap[@id='").append(id).append("']");
            IPropertyTree *mapTree = root->queryPropTree(xpath);
            Owned<IEspPackageListMapData> res = createPackageListMapData("", "");
            res->setActive(item.getPropBool("@active"));
            getPackageListInfo(mapTree, res);
            results->append(*res.getClear());
        }
    }
}
void getPkgInfo(const char *target, const char *process, StringBuffer &info)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        throw MakeStringException(PKG_DALI_LOOKUP_ERROR, "Unable to retrieve package information from dali /PackageMaps");
    IPropertyTree *root = globalLock->queryRoot();
    Owned<IPropertyTree> tree = createPTree("PackageMaps");
    Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry((process && *process) ? process : "*", NULL, true);
    StringBuffer xpath("PackageMap[@active='1']");
    if (target && *target)
        xpath.appendf("[@querySet='%s']", target);
    Owned<IPropertyTreeIterator> iter = pkgSetRegistry->getElements(xpath.str());
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        const char *id = item.queryProp("@id");
        if (id)
        {
            StringBuffer xpath;
            xpath.append("PackageMap[@id='").append(id).append("']");
            IPropertyTree *mapTree = root->queryPropTree(xpath);
            if (mapTree)
                mergePTree(tree, mapTree);
        }
    }

    toXML(tree, info);
}

bool deletePkgInfo(const char *packageMap, const char *target)
{
    Owned<IRemoteConnection> pkgSet = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!pkgSet)
        throw MakeStringException(PKG_NONE_DEFINED, "No package sets defined");

    IPropertyTree* packageSets = pkgSet->queryRoot();

    VStringBuffer pkgSet_xpath("PackageSet[@id='%s']", target);
    IPropertyTree *pkgSetRegistry = packageSets->queryPropTree(pkgSet_xpath.str());
    if (!pkgSetRegistry)
        throw MakeStringException(PKG_TARGET_NOT_DEFINED, "No package sets defined for %s", target);

    StringBuffer lcName(packageMap);
    lcName.toLowerCase();
    VStringBuffer xpath("PackageMap[@id='%s'][@querySet='%s']", lcName.str(), target);
    IPropertyTree *pm = pkgSetRegistry->getPropTree(xpath.str());
    if (pm)
        pkgSetRegistry->removeTree(pm);
    else
        throw MakeStringException(PKG_DELETE_NOT_FOUND, "Unable to delete %s - information not found", lcName.str());

    VStringBuffer ps_xpath("PackageSet/PackageMap[@id='%s']", lcName.str());

    if (!packageSets->hasProp(ps_xpath))
    {
        Owned<IRemoteConnection> pkgMap = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        if (pkgMap)
        {
            VStringBuffer map_xpath("PackageMap[@id='%s']", lcName.str());
            IPropertyTree *pkgMaproot = pkgMap->queryRoot();
            IPropertyTree *pm = pkgMaproot->getPropTree(map_xpath.str());
            if (pm)
                pkgMaproot->removeTree(pm);
        }
    }
    return true;
}

void activatePackageMapInfo(const char *target, const char *packageMap, bool activate)
{
    if (!target || !*target)
        throw MakeStringException(PKG_TARGET_NOT_DEFINED, "No target defined");

    Owned<IRemoteConnection> globalLock = querySDS().connect("PackageSets", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        throw MakeStringException(PKG_DALI_LOOKUP_ERROR, "Unable to retrieve PackageSets information from dali /PackageSets");

    StringBuffer lcName(target);
    lcName.toLowerCase();
    VStringBuffer xpath("PackageSet[@id=\"%s\"]", lcName.str());

    IPropertyTree *root = globalLock->queryRoot();
    if (!root)
        throw MakeStringException(PKG_ACTIVATE_NOT_FOUND, "Unable to retrieve PackageSet information for %s", lcName.str());

    IPropertyTree *pkgSetTree = root->queryPropTree(xpath);
    if (pkgSetTree)
    {
        if (packageMap && *packageMap)
        {
            StringBuffer lcMapName(packageMap);
            lcMapName.toLowerCase();
            VStringBuffer xpath_map("PackageMap[@id=\"%s\"]", lcMapName.str());
            IPropertyTree *mapTree = pkgSetTree->queryPropTree(xpath_map);
            if (activate)
                makePackageActive(pkgSetTree, mapTree, lcName.str());
            else
                mapTree->setPropBool("@active", false);
        }
    }
}

bool CWsPackageProcessEx::onAddPackage(IEspContext &context, IEspAddPackageRequest &req, IEspAddPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer info(req.getInfo());
    bool activate = req.getActivate();
    bool overWrite = req.getOverWrite();
    StringAttr target(req.getTarget());
    StringAttr pkgMapName(req.getPackageMap());
    StringAttr processName(req.getProcess());

    Owned<IUserDescriptor> userdesc;
    const char *user = context.queryUserId();
    const char *password = context.queryPassword();
    if (user && *user && *password && *password)
    {
        userdesc.setown(createUserDescriptor());
        userdesc->set(user, password);
    }

    VStringBuffer pkgSetId("default_%s", processName.get());
    pkgSetId.replace('*', '#');
    pkgSetId.replace('?', '~');

    Owned<IPropertyTree> packageTree = createPTreeFromXMLString(info.str());
    Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry(pkgSetId.str(), processName.get(), false);
    addPackageMapInfo(pkgSetRegistry, target.get(), pkgMapName.get(), pkgSetId.str(), req.getDaliIp(), LINK(packageTree), activate, overWrite, userdesc);

    StringBuffer msg;
    msg.append("Successfully loaded ").append(pkgMapName.get());
    resp.updateStatus().setDescription(msg.str());
    return true;
}

bool CWsPackageProcessEx::onDeletePackage(IEspContext &context, IEspDeletePackageRequest &req, IEspDeletePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringAttr pkgMap(req.getPackageMap());
    bool ret = deletePkgInfo(pkgMap.get(), req.getTarget());
    StringBuffer msg;
    (ret) ? msg.append("Successfully ") : msg.append("Unsuccessfully ");
    msg.append("deleted ").append(pkgMap.get()).append(" from ").append(req.getTarget());

    resp.updateStatus().setDescription(msg.str());
    return true;
}

bool CWsPackageProcessEx::onActivatePackage(IEspContext &context, IEspActivatePackageRequest &req, IEspActivatePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer target(req.getTarget());
    StringBuffer pkgMapName(req.getPackageMap());

    activatePackageMapInfo(target.str(), pkgMapName.str(), true);
    return true;
}

bool CWsPackageProcessEx::onDeActivatePackage(IEspContext &context, IEspDeActivatePackageRequest &req, IEspDeActivatePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer target(req.getTarget());
    StringBuffer pkgMapName(req.getPackageMap());

    activatePackageMapInfo(target.str(), pkgMapName.str(), false);
    return true;
}

bool CWsPackageProcessEx::onListPackage(IEspContext &context, IEspListPackageRequest &req, IEspListPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    IArrayOf<IConstPackageListMapData> results;
    StringAttr process(req.getProcess());
    listPkgInfo(req.getTarget(), process.length() ? process.get() : "*", &results);
    resp.setPkgListMapData(results);
    return true;
}

bool CWsPackageProcessEx::onGetPackage(IEspContext &context, IEspGetPackageRequest &req, IEspGetPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringAttr process(req.getProcess());
    StringBuffer info;
    getPkgInfo(req.getTarget(), process.length() ? process.get() : "*", info);
    resp.setInfo(info);
    return true;
}
