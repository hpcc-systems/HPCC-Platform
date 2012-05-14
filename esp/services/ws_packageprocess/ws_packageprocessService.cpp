/*##############################################################################
Copyright (C) 2011 HPCC Systems.
############################################################################## */

#pragma warning (disable : 4786)

#include "ws_packageprocessService.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dfuutil.hpp"
#include "ws_fs.hpp"
#include "ws_workunits.hpp"

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

IPropertyTree *getPkgSetRegistry(const char *setName, bool readonly)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(setName).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        if (readonly)
            return NULL;
        Owned<IPropertyTree> querySet = createPTree();
        querySet->setProp("@id", setName);
        globalLock->queryRoot()->addPropTree("PackageSet", querySet.getClear());
        globalLock->commit();

        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!conn)
            throwUnexpected();
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

bool isFileKnownOnCluster(const char *logicalname, const char *lookupDaliIp, const char *process, IUserDescriptor* userdesc)
{
    Owned<IDistributedFile> dst = queryDistributedFileDirectory().lookup(logicalname, userdesc, true);
    if (dst)
    {
        if (dst->findCluster(process) != NotFound)
            return true; // file already known for this cluster
    }
    return false;
}

bool addFileInfoToDali(const char *logicalname, const char *lookupDaliIp, const char *process, bool overwrite, IUserDescriptor* userdesc, StringBuffer &host, short port, StringBuffer &msg)
{
    bool retval = true;
    try
    {
        if (!overwrite)
        {
            if (isFileKnownOnCluster(logicalname, lookupDaliIp, process, userdesc))
                return true;
        }

        StringBuffer user;
        StringBuffer password;

        if (userdesc)
        {
            userdesc->getUserName(user);
            userdesc->getPassword(password);
        }

        Owned<IClientFileSpray> fs;
        fs.setown(createFileSprayClient());
        fs->setUsernameToken(user.str(), password.str(), NULL);

        VStringBuffer url("http://%s:%d/FileSpray", host.str(), port);
        fs->addServiceUrl(url.str());

        bool isRoxie = isRoxieProcess(process);

        Owned<IClientCopy> req = fs->createCopyRequest();
        req->setSourceLogicalName(logicalname);
        req->setDestLogicalName(logicalname);
        req->setDestGroup(process);
        req->setSuperCopy(false);
        if (isRoxie)
            req->setDestGroupRoxie("Yes");

        req->setSourceDali(lookupDaliIp);

        req->setSrcusername(user);
        req->setSrcpassword(password);
        req->setOverwrite(overwrite);

        Owned<IClientCopyResponse> resp = fs->Copy(req);
    }
    catch(IException *e)
    {
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
//////////////////////////////////////////////////////////

void addPackageMapInfo(IPropertyTree *pkgSetRegistry, const char *setName, const char *packageSetName, IPropertyTree *packageInfo, bool active, bool overWrite)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    StringBuffer lcName(packageSetName);
    lcName.toLowerCase();
    StringBuffer xpath;
    xpath.append("PackageMap[@id='").append(lcName).append("']");

    IPropertyTree *pkgRegTree = pkgSetRegistry->queryPropTree(xpath.str());

    IPropertyTree *root = globalLock->queryRoot();
    IPropertyTree *mapTree = root->queryPropTree(xpath);

    if (!overWrite && (pkgRegTree || mapTree))
    {
        throw MakeStringException(0, "Package name %s already exists, either delete it or specify overwrite", lcName.str());
    }

    if (mapTree)
        root->removeTree(mapTree);

    if (pkgRegTree)
        pkgSetRegistry->removeTree(pkgRegTree);


    mapTree = root->addPropTree("PackageMap", createPTree());
    mapTree->addProp("@id", packageSetName);

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
                StringAttr id(supertree.queryProp("@id"));
                if (id.length() && id[0] == '~')
                    supertree.setProp("@id", id+1);

                Owned<IPropertyTreeIterator> sub_iter = supertree.getElements("SubFile");
                ForEach(*sub_iter)
                {
                    IPropertyTree &subtree = sub_iter->query();
                    StringAttr subid = subtree.queryProp("@value");
                    if (subid.length())
                    {
                        if (subid[0] == '~')
                            subtree.setProp("@value", subid+1);
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
    globalLock->commit();

    IPropertyTree *pkgSetTree = pkgSetRegistry->addPropTree("PackageMap", createPTree("PackageMap"));
    pkgSetTree->setProp("@id", lcName);
    pkgSetTree->setProp("@querySet", setName);
    pkgSetTree->setPropBool("@active", active);
}

void copyPackageSubFiles(IPropertyTree *packageInfo, const char *process, const char *defaultLookupDaliIp, bool overwrite, IUserDescriptor* userdesc, StringBuffer &host, short port)
{
    Owned<IPropertyTreeIterator> iter = packageInfo->getElements("Package");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        StringBuffer lookupDaliIp;
        lookupDaliIp.append(item.queryProp("@daliip"));
        if (lookupDaliIp.length() == 0)
            lookupDaliIp.append(defaultLookupDaliIp);
        if (lookupDaliIp.length() == 0)
        {
            StringAttr superfile(item.queryProp("@id"));
            DBGLOG("Could not lookup SubFiles in package %s because no remote dali ip was specified", superfile.get());
            return;
        }
        Owned<IPropertyTreeIterator> super_iter = item.getElements("SuperFile");
        ForEach(*super_iter)
        {
            IPropertyTree &supertree = super_iter->query();
            Owned<IPropertyTreeIterator> sub_iter = supertree.getElements("SubFile");
            ForEach(*sub_iter)
            {
                IPropertyTree &subtree = sub_iter->query();
                StringAttr subid = subtree.queryProp("@value");
                if (subid.length())
                {
                    StringBuffer msg;
                    addFileInfoToDali(subid.get(), lookupDaliIp, process, overwrite, userdesc, host, port, msg);
                }
            }
        }
    }
}

void getPackageListInfo(IPropertyTree *mapTree, IEspPackageListMapData *pkgList)
{
    pkgList->setId(mapTree->queryProp("@id"));

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
void listPkgInfo(const char *cluster, IArrayOf<IConstPackageListMapData>* results)
{
    StringBuffer info;
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        return;
    IPropertyTree *root = globalLock->queryRoot();
    if (!cluster || !*cluster)
    {
        info.append("<PackageMaps>");
        Owned<IPropertyTreeIterator> iter = root->getElements("PackageMap");
        ForEach(*iter)
        {
            Owned<IEspPackageListMapData> res = createPackageListMapData("", "");
            IPropertyTree &item = iter->query();
            getPackageListInfo(&item, res);
            results->append(*res.getClear());
        }
        info.append("</PackageMaps>");
    }
    else
    {
        Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry(cluster, true);
        Owned<IPropertyTreeIterator> iter = pkgSetRegistry->getElements("PackageMap");
        info.append("<PackageMaps>");
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
                getPackageListInfo(mapTree, res);
                results->append(*res.getClear());
            }
        }
        info.append("</PackageMaps>");
    }
}
void getPkgInfo(const char *cluster, const char *package, StringBuffer &info)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageMaps/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        return;
    IPropertyTree *root = globalLock->queryRoot();
    Owned<IPropertyTree> tree = createPTree("PackageMaps");
    if (cluster)
    {
        Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry(cluster, true);
        Owned<IPropertyTreeIterator> iter = pkgSetRegistry->getElements("PackageMap[@active='1']");
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
    }
    else
    {
        StringBuffer xpath;
        xpath.append("PackageMap[@id='").append(package).append("']");
        Owned<IPropertyTreeIterator> iter = root->getElements(xpath.str());
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            mergePTree(tree, &item);
        }
    }
    toXML(tree, info);
}

bool deletePkgInfo(const char *packageSetName, const char *queryset)
{
    Owned<IRemoteConnection> pkgSet = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!pkgSet)
    {
        DBGLOG("No package sets defined");
        return false;
    }

    IPropertyTree* packageSets = pkgSet->queryRoot();

    VStringBuffer pkgSet_xpath("PackageSet[@id='%s']", queryset);
    IPropertyTree *pkgSetRegistry = packageSets->queryPropTree(pkgSet_xpath.str());
    if (!pkgSetRegistry)
    {
        DBGLOG("No package sets defined for = %s", queryset);
        return false;
    }

    StringBuffer lcName(packageSetName);
    lcName.toLowerCase();
    VStringBuffer xpath("PackageMap[@id='%s'][@querySet='%s']", lcName.str(), queryset);
    IPropertyTree *pm = pkgSetRegistry->getPropTree(xpath.str());
    if (pm)
        pkgSetRegistry->removeTree(pm);

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

void activatePackageMapInfo(const char *packageSetName, const char *packageMap, bool activate)
{
    if (!packageSetName || !*packageSetName)
        return;

    Owned<IRemoteConnection> globalLock = querySDS().connect("PackageSets", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        return;

    StringBuffer lcName(packageSetName);
    lcName.toLowerCase();
    VStringBuffer xpath("PackageSet[@id=\"%s\"]", lcName.str());

    IPropertyTree *root = globalLock->queryRoot();
    if (!root)
        return;

    IPropertyTree *pkgSetTree = root->queryPropTree(xpath);
    if (pkgSetTree)
    {
        if (packageMap && *packageMap)
        {
            StringBuffer lcMapName(packageMap);
            lcMapName.toLowerCase();
            VStringBuffer xpath_map("PackageMap[@id=\"%s\"]", lcMapName.str());
            IPropertyTree *mapTree = pkgSetTree->queryPropTree(xpath_map);
            mapTree->setPropBool("@active", activate);
        }
        else
        {
            Owned<IPropertyTreeIterator> iter = pkgSetTree->getElements("PackageMap");
            ForEach(*iter)
            {
                IPropertyTree &item = iter->query();
                item.setPropBool("@active", activate);
            }
        }
    }
}

bool CWsPackageProcessEx::onAddPackage(IEspContext &context, IEspAddPackageRequest &req, IEspAddPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer info(req.getInfo());
    bool activate = req.getActivate();
    bool overWrite = req.getOverWrite();
    StringAttr querySet(req.getQuerySet());
    StringAttr pkgName(req.getPackageName());

    Owned<IPropertyTree> packageTree = createPTreeFromXMLString(info.str());
    Owned<IPropertyTree> pkgSetRegistry = getPkgSetRegistry(querySet.get(), false);
    addPackageMapInfo(pkgSetRegistry, querySet.get(), pkgName.get(), LINK(packageTree), activate, overWrite);

    StringBuffer msg;
    msg.append("Successfully loaded ").append(pkgName.get());
    resp.updateStatus().setDescription(msg.str());
    return true;
}

bool CWsPackageProcessEx::onDeletePackage(IEspContext &context, IEspDeletePackageRequest &req, IEspDeletePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringAttr pkgName(req.getPackageName());
    bool ret = deletePkgInfo(pkgName.get(), req.getQuerySet());
    StringBuffer msg;
    (ret) ? msg.append("Successfully ") : msg.append("Unsuccessfully ");
    msg.append("deleted").append(pkgName.get());

    resp.updateStatus().setDescription(msg.str());
    return true;
}

bool CWsPackageProcessEx::onActivatePackage(IEspContext &context, IEspActivatePackageRequest &req, IEspActivatePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer pkgName(req.getPackageName());
    StringBuffer pkgMapName(req.getPackageMapName());

    activatePackageMapInfo(pkgName.str(), pkgMapName.str(), true);
    return true;
}

bool CWsPackageProcessEx::onDeActivatePackage(IEspContext &context, IEspDeActivatePackageRequest &req, IEspDeActivatePackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer pkgName(req.getPackageName());
    StringBuffer pkgMapName(req.getPackageMapName());

    activatePackageMapInfo(pkgName.str(), pkgMapName.str(), false);
    return true;
}

bool CWsPackageProcessEx::onListPackage(IEspContext &context, IEspListPackageRequest &req, IEspListPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    IArrayOf<IConstPackageListMapData> results;
    listPkgInfo(req.getCluster(), &results);
    resp.setPkgListMapData(results);
    return true;
}

bool CWsPackageProcessEx::onGetPackage(IEspContext &context, IEspGetPackageRequest &req, IEspGetPackageResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringAttr cluster(req.getCluster());
    StringAttr pkgName(req.getPackageName());
    StringBuffer info;
    getPkgInfo(cluster.length() ? cluster.get() : NULL, pkgName.length() ? pkgName.get() : NULL, info);
    resp.setInfo(info);
    return true;
}

bool CWsPackageProcessEx::onCopyFiles(IEspContext &context, IEspCopyFilesRequest &req, IEspCopyFilesResponse &resp)
{
    resp.updateStatus().setCode(0);
    StringBuffer info(req.getInfo());
    StringAttr process(req.getProcess());
    StringAttr pkgName(req.getPackageName());
    StringAttr lookupDaliIp(req.getDaliIp());

    if (process.length() == 0)
        throw MakeStringException(0, "CWsPackageProcessEx::onCopyFiles process parameter not set.");

    Owned<IUserDescriptor> userdesc;
    const char *user = context.queryUserId();
    const char *password = context.queryPassword();
    if (user && *user && *password && *password)
    {
        userdesc.setown(createUserDescriptor());
        userdesc->set(user, password);
    }

    StringBuffer host;
    short port;
    context.getServAddress(host, port);

    Owned<IPropertyTree> packageTree = createPTreeFromXMLString(info.str());
    copyPackageSubFiles(LINK(packageTree), process, lookupDaliIp, req.getOverWrite(), userdesc, host, port);

    StringBuffer msg;
    msg.append("Successfully loaded ").append(pkgName.get());
    resp.updateStatus().setDescription(msg.str());
    return true;
}
