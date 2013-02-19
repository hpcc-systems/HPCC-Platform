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


#include "platform.h"
#include "jprop.hpp"
#include "jptree.hpp"
#include "pkgimpl.hpp"

#include "dadfs.hpp"
#include "eclrtl.hpp"
#include "jregexp.hpp"
#include "dasds.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

//wrap the hashing function here to simplify template dependencies
hash64_t pkgHash64Data(size32_t len, const void *buf, hash64_t hval)
{
    return rtlHash64Data(len, buf, hval);
}

CPackageNode::CPackageNode(IPropertyTree *p)
{
    if (p)
        node.set(p);
    else
        node.setown(createPTree("HpccPackages"));
    StringBuffer xml;
    toXML(node, xml);
    hash = rtlHash64Data(xml.length(), xml.str(), 9994410);
}

    // Merge base package environment into mergedEnvironment
void CPackageNode::mergeEnvironment(const CPackageNode *base)
{
    Owned<IPropertyIterator> envIterator = base->mergedEnvironment->getIterator();
    ForEach(*envIterator)
    {
        const char *id = envIterator->getPropKey();
        const char *val = base->mergedEnvironment->queryProp(id);
        if (id && val && !mergedEnvironment->hasProp(id))
            mergedEnvironment->setProp(id, val);
    }
}

// Use local package and its bases to resolve superfile name list of subfiles via all supported resolvers
ISimpleSuperFileEnquiry *CPackageNode::resolveSuperFile(const char *superFileName) const
{
    // Order of resolution:
    // 1. SuperFiles named in local package
    // 2. SuperFiles named in bases
    // There is no dali or local case - a superfile that is resolved in dali must also resolve the subfiles there (and is all done in the resolveLFNusingDali method)
    if (superFileName && *superFileName && node)
    {
        if (*superFileName=='~')
            superFileName++;
        StringBuffer xpath;
        Owned<IPropertyTreeIterator> subFiles = lookupElements(makeSuperFileXPath(xpath, superFileName), "SubFile");
        if (subFiles)
        {
            Owned<CPackageSuperFileArray> result = new CPackageSuperFileArray(*subFiles);
            return result.getClear();
        }
    }
    return NULL;
}

// Load mergedEnvironment from local XML node
void CPackageNode::loadEnvironment()
{
    mergedEnvironment.setown(createProperties(true));
    Owned<IPropertyTreeIterator> envIterator = node->getElements("Environment");
    ForEach(*envIterator)
    {
        IPropertyTree &env = envIterator->query();
        const char *id = env.queryProp("@id");
        const char *val = env.queryProp("@value");
        if (!val)
            val = env.queryProp("@val"); // Historically we used val here - not sure why... other parts of package file used value
        if (id && val)
            mergedEnvironment->setProp(id, val);
        else
        {
            StringBuffer s;
            toXML(&env, s);
            throw MakeStringException(PACKAGE_MISSING_ID, "PACKAGE_ERROR: Environment element missing id or value: %s", s.str());
        }
    }
    Owned<IAttributeIterator> attrs = node->getAttributes();
    for(attrs->first(); attrs->isValid(); attrs->next())
    {
        StringBuffer s("control:");
        s.append(attrs->queryName()+1);  // queryName() has a leading @, hence the +1
        mergedEnvironment->setProp(s.str(), attrs->queryValue());
    }
}

bool CPackageNode::validate(IMultiException *me) const
{
    if (!node)
        return true;
    const char *packageId = node->queryProp("@id");
    if (!packageId || !*packageId)
        me->append(*MakeStringExceptionDirect(PACKAGE_MISSING_ID, "Package has no id attribute"));
    Owned<IPropertyTreeIterator> files = node->getElements("SuperFile");
    ForEach(*files)
    {
        IPropertyTree &super = files->query();
        const char *superId = super.queryProp("@id");
        if (!superId || !*superId)
            me->append(*MakeStringExceptionDirect(PACKAGE_MISSING_ID, "SuperFile has no id attribute"));

        if (!super.hasProp("SubFile"))
            me->append(*MakeStringException(PACKAGE_NO_SUBFILES, "Warning: Package['%s']/SuperFile['%s'] has no SubFiles defined", packageId, superId ? superId : ""));
    }
    return true;
}


CHpccPackage *createPackage(IPropertyTree *p)
{
    return new CHpccPackage(p);
}

//================================================================================================
// CPackageMap - an implementation of IPackageMap using a string map
//================================================================================================

IHpccPackageMap *createPackageMapFromPtree(IPropertyTree *t, const char *queryset, const char *id)
{
    Owned<CHpccPackageMap> pm = new CHpccPackageMap(id, queryset, true);
    pm->load(t);
    return pm.getClear();
}

IHpccPackageMap *createPackageMapFromXml(const char *xml, const char *queryset, const char *id)
{
    Owned<IPropertyTree> t = createPTreeFromXMLString(xml);
    return createPackageMapFromPtree(t, queryset, id);
}


//================================================================================================
// CHpccPackageSet - an implementation of IHpccPackageSet
//================================================================================================

CHpccPackageSet::CHpccPackageSet(const char *_process) : process(_process)
{
    Owned<IPropertyTree> ps = resolvePackageSetRegistry(process, true);
    if (ps)
        load(ps);
}

void CHpccPackageSet::load(IPropertyTree *xml)
{
    Owned<IPropertyTreeIterator> it = xml->getElements("PackageMap[@active='1']"); //only active for now
    ForEach(*it)
    {
        IPropertyTree &tree = it->query();
        if (!tree.hasProp("@id"))
            continue;
        Owned<CHpccPackageMap> pm = new CHpccPackageMap(tree.queryProp("@id"), tree.queryProp("@querySet"), true);
        pm->load(tree.queryProp("@id"));
        packageMaps.append(*pm.getClear());
    }
}

const IHpccPackageMap *CHpccPackageSet::queryActiveMap(const char *queryset) const
{
    ForEachItemIn(i, packageMaps)
    {
        CHpccPackageMap &pm = packageMaps.item(i);
        StringAttr &match = pm.querySet;
        if (!match.length())
            continue;
        if (isWildString(match))
        {
            if (WildMatch(queryset, match))
                return &pm;
        }
        else if (streq(queryset, match))
            return &pm;
    }
    return NULL;
}

extern WORKUNIT_API IHpccPackageSet *createPackageSet(const char *process)
{
    return new CHpccPackageSet(process);
}

extern WORKUNIT_API IPropertyTree * getPackageMapById(const char * id, bool readonly)
{
    StringBuffer xpath;
    xpath.append("/PackageMaps/PackageMap[@id=\"").append(id).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    return conn->getRoot();
}

extern WORKUNIT_API IPropertyTree * getPackageSetById(const char * id, bool readonly)
{
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(id).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    return conn->getRoot();
}

extern WORKUNIT_API IPropertyTree * resolvePackageSetRegistry(const char *process, bool readonly)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    Owned<IPropertyTree> psroot = globalLock->getRoot();

    StringBuffer xpath;
    xpath.append("PackageSet[@process=\"").append(process).append("\"]");
    IPropertyTree *ps = psroot->queryPropTree(xpath);
    if (ps)
        return getPackageSetById(ps->queryProp("@id"), readonly);
    Owned<IPropertyTreeIterator> it = psroot->getElements("PackageSet[@process]");
    ForEach(*it)
    {
        const char *match = it->query().queryProp("@process");
        const char *id = it->query().queryProp("@id");
        if (id && isWildString(match) && WildMatch(process, match))
            return getPackageSetById(id, readonly);
    }
    return NULL;
}
