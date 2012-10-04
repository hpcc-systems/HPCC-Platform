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
#include "package.hpp"

#include "dadfs.hpp"
#include "eclrtl.hpp"
#include "jregexp.hpp"
#include "dasds.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

static IPropertyTree * getPackageMapById(const char * id, bool readonly)
{
    StringBuffer xpath;
    xpath.append("/PackageMaps/PackageMap[@id=\"").append(id).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    return conn->getRoot();
}

static IPropertyTree * getPackageSetById(const char * id, bool readonly)
{
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(id).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    return conn->getRoot();
}

static IPropertyTree * resolvePackageSetRegistry(const char *process, bool readonly)
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

class CPackageSuperFileArray : public CInterface, implements ISimpleSuperFileEnquiry
{
    IArrayOf<IPropertyTree> subFiles;
public:
    IMPLEMENT_IINTERFACE;
    CPackageSuperFileArray(IPropertyTreeIterator &_subs)
    {
        ForEach(_subs)
        {
            IPropertyTree &sub = _subs.query();
            sub.Link();
            subFiles.append(sub);
        }
    }
    virtual unsigned numSubFiles() const
    {
        return subFiles.length();
    }
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const
    {
        if (subFiles.isItem(num))
        {
            name.append(subFiles.item(num).queryProp("@value"));
            return true;
        }
        else
            return false;
    }
    virtual unsigned findSubName(const char *subname) const
    {
        ForEachItemIn(idx, subFiles)
        {
            if (stricmp(subFiles.item(idx).queryProp("@value"), subname))
                return idx;
        }
        return NotFound;
    }
    virtual unsigned getContents(StringArray &contents) const
    {
        ForEachItemIn(idx, subFiles)
        {
            contents.append(subFiles.item(idx).queryProp("@value"));
        }
        return subFiles.length();
    }
};



class CHpccPackage : extends CInterface, implements IHpccPackage
{
protected:
    Owned<IPropertyTree> node;
    IArrayOf<CHpccPackage> bases;
    Owned<IProperties> mergedEnvironment;
    hash64_t hash;

    // Merge base package environment into mergedEnvironment
    void mergeEnvironment(const CHpccPackage *base)
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

    // Search this package and any bases for an element matching xpath1, then return iterator for its children that match xpath2
    IPropertyTreeIterator *lookupElements(const char *xpath1, const char *xpath2) const
    {
        IPropertyTree *parentNode = node->queryPropTree(xpath1);
        if (parentNode)
            return parentNode->getElements(xpath2);
        ForEachItemIn(idx, bases)
        {
            const CHpccPackage &basePackage = bases.item(idx);
            IPropertyTreeIterator *it = basePackage.lookupElements(xpath1, xpath2);
            if (it)
                return it;
        }
        return NULL;
    }

    inline StringBuffer makeSuperFileXPath(StringBuffer &xpath, const char *superFileName) const
    {
        return xpath.append("SuperFile[@id='").appendLower(strlen(superFileName), superFileName).append("']");
    }

    // Use local package and its bases to resolve superfile name list of subfiles via all supported resolvers
    ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const
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

    inline bool hasProp(const char *xpath) const
    {
        return (node) ? node->hasProp(xpath) : false;
    }

    bool hasSuperFile(const char *superFileName) const
    {
        if (!superFileName || !*superFileName || !node)
            return false;

        if (*superFileName=='~')
            superFileName++;
        StringBuffer xpath;
        if (hasProp(makeSuperFileXPath(xpath, superFileName)))
            return true;
        ForEachItemIn(idx, bases)
        {
            if (bases.item(idx).hasProp(xpath))
                return true;
        }

        return false;
    }

    CHpccPackage()
    {
        hash = 0;
    }

public:
    IMPLEMENT_IINTERFACE;

    CHpccPackage(IPropertyTree *p)
    {
        if (p)
            node.set(p);
        else
            node.setown(createPTree("HpccPackages"));
        StringBuffer xml;
        toXML(node, xml);
        hash = rtlHash64Data(xml.length(), xml.str(), 9994410);
    }

    ~CHpccPackage()
    {
    }
    // Load mergedEnvironment from local XML node
    void loadEnvironment()
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
                throw MakeStringException(0, "PACKAGE_ERROR: Environment element missing id or value: %s", s.str());
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

    virtual void resolveBases(IHpccPackageMap *packages)
    {
        loadEnvironment();
        if (packages)
        {
            Owned<IPropertyTreeIterator> baseIterator = node->getElements("Base");
            if (baseIterator->first())
            {
                do
                {
                    IPropertyTree &baseElem = baseIterator->query();
                    const char *baseId = baseElem.queryProp("@id");
                    if (!baseId)
                        throw MakeStringException(0, "PACKAGE_ERROR: base element missing id attribute");
                    const IHpccPackage *_base = packages->queryPackage(baseId);
                    if (_base)
                    {
                        const CHpccPackage *base = static_cast<const CHpccPackage *>(_base);
                        bases.append(const_cast<CHpccPackage &>(*LINK(base)));   // should really be an arrayof<const base> but that would require some fixing in jlib
                        hash = rtlHash64Data(sizeof(base->hash), &base->hash, hash);
                        mergeEnvironment(base);
                    }
                    else
                        throw MakeStringException(0, "PACKAGE_ERROR: base package %s not found", baseId);
                }
                while(baseIterator->next());
            }
        }
    }

    virtual const char *queryEnv(const char *varname) const
    {
        return mergedEnvironment->queryProp(varname);
    }

    virtual hash64_t queryHash() const
    {
        return hash;
    }

    virtual const IPropertyTree *queryTree() const
    {
        return node;
    }

    virtual IPropertyTree *getQuerySets() const
    {
        if (node)
            return node->getPropTree("QuerySets");
        else
            return NULL;
    }
};

CHpccPackage *createPackage(IPropertyTree *p)
{
    return new CHpccPackage(p);
}

//================================================================================================
// CPackageMap - an implementation of IPackageMap using a string map
//================================================================================================

class CHpccPackageMap : public CInterface, implements IHpccPackageMap
{
public:
    MapStringToMyClass<CHpccPackage> packages;
    StringAttr packageId;
    StringAttr querySet;
    bool active;
    StringArray wildMatches, wildIds;
public:
    IMPLEMENT_IINTERFACE;
    CHpccPackageMap(const char *_packageId, const char *_querySet, bool _active)
        : packageId(_packageId), querySet(_querySet), active(_active), packages(true)
    {
    }

    // IPackageMap interface
    virtual bool isActive() const
    {
        return active;
    }
    virtual const IHpccPackage *queryPackage(const char *name) const
    {
        return name ? packages.getValue(name) : NULL;
    }
    virtual const IHpccPackage *matchPackage(const char *name) const
    {
        if (name)
        {
            const IHpccPackage *pkg = queryPackage(name);
            if (pkg)
                return pkg;
            ForEachItemIn(idx, wildMatches)
            {
                if (WildMatch(name, wildMatches.item(idx), true))
                    return queryPackage(wildIds.item(idx));
            }
        }
        return NULL;
    }
    virtual const char *queryPackageId() const
    {
        return packageId;
    }
    void load(const char *id)
    {
        load(getPackageMapById(id, true));
    }
    void load(IPropertyTree *xml)
    {
        if (!xml)
            return;
        Owned<IPropertyTreeIterator> allpackages = xml->getElements("Package");
        ForEach(*allpackages)
        {
            IPropertyTree &packageTree = allpackages->query();
            const char *id = packageTree.queryProp("@id");
            if (!id || !*id)
                throw MakeStringException(-1, "Invalid package map - Package element missing id attribute");
            Owned<CHpccPackage> package = createPackage(&packageTree);
            packages.setValue(id, package.get());
            const char *queries = packageTree.queryProp("@queries");
            if (queries && *queries)
            {
                wildMatches.append(queries);
                wildIds.append(id);
            }
        }
        HashIterator it(packages);
        ForEach (it)
        {
            CHpccPackage *pkg = packages.mapToValue(&it.query());
            if (pkg)
                pkg->resolveBases(this);
        }
    }
};

//================================================================================================
// CHpccPackageSet - an implementation of IHpccPackageSet
//================================================================================================

class CHpccPackageSet : public CInterface, implements IHpccPackageSet
{
    IArrayOf<CHpccPackageMap> packageMaps;
    StringAttr process;
public:
    IMPLEMENT_IINTERFACE;
    CHpccPackageSet(const char *_process)
        : process(_process)
    {
        Owned<IPropertyTree> ps = resolvePackageSetRegistry(process, true);
        if (ps)
            load(ps);
    }

    void load(IPropertyTree *xml)
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

    virtual const IHpccPackageMap *queryActiveMap(const char *queryset) const
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
};

extern WORKUNIT_API IHpccPackageSet *createPackageSet(const char *process)
{
    return new CHpccPackageSet(process);
}
