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

#ifndef WUPACKAGE_IMPL_HPP
#define WUPACKAGE_IMPL_HPP

#include "platform.h"
#include "jprop.hpp"
#include "jptree.hpp"
#include "jregexp.hpp"
#include "package.h"

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

class WORKUNIT_API CPackageNode : extends CInterface, implements IHpccPackage
{
protected:
    Owned<IPropertyTree> node;
    Owned<IProperties> mergedEnvironment;
    hash64_t hash;

    void mergeEnvironment(const CPackageNode *base);

    virtual IPropertyTreeIterator *lookupElements(const char *xpath1, const char *xpath2) const = 0;

    inline StringBuffer makeSuperFileXPath(StringBuffer &xpath, const char *superFileName) const
    {
        return xpath.append("SuperFile[@id='").appendLower(strlen(superFileName), superFileName).append("']");
    }

    ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const;

    inline bool hasProp(const char *xpath) const
    {
        return (node) ? node->hasProp(xpath) : false;
    }

    virtual bool getSysFieldTranslationEnabled() const {return false;}
    virtual bool getEnableFieldTranslation() const
    {
        const char *val = queryEnv("control:enableFieldTranslation");
        if (val)
            return strToBool(val);
        else
            return getSysFieldTranslationEnabled();
    }


    CPackageNode()
    {
        hash = 0;
    }

    virtual const IHpccPackage *queryRootPackage()
    {
        return NULL;
    }


public:
    IMPLEMENT_IINTERFACE;

    CPackageNode(IPropertyTree *p);

    ~CPackageNode()
    {
    }

    // Load mergedEnvironment from local XML node
    void loadEnvironment();

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
        if (!node)
            return NULL;
        return node->getPropTree("QuerySets");
    }
};

template <class TYPE>
class CResolvedPackage : public TYPE
{
public:
    typedef CResolvedPackage<TYPE> self;
    CIArrayOf<self> bases;

    CResolvedPackage<TYPE>(IPropertyTree *p) : TYPE(p) {}

    virtual aindex_t getBaseCount() const {return bases.length();}
    const self *getResolvedBase(aindex_t pos) const
    {
        if (pos < getBaseCount())
            return &bases.item(pos);
        return NULL;
    }
    virtual const TYPE *getBaseNode(aindex_t pos) const {return (const TYPE *) getResolvedBase(pos);}

    void appendBase(const IHpccPackage *base)
    {
        if (base)
        {
            const self *p = dynamic_cast<const self *>(base);
            bases.append(const_cast<self &>(*LINK(p)));   // should really be an arrayof<const base> but that would require some fixing in jlib
            TYPE::hash = pkgHash64Data(sizeof(p->hash), &p->hash, TYPE::hash);
            TYPE::mergeEnvironment(p);
        }
    }

    void resolveBases(const IHpccPackageMap *packages)
    {
        TYPE::loadEnvironment();
        if (packages)
        {
            Owned<IPropertyTreeIterator> baseIterator = TYPE::node->getElements("Base");
            if (!baseIterator->first())
                appendBase(TYPE::queryRootPackage());
            else
            {
                do
                {
                    IPropertyTree &baseElem = baseIterator->query();
                    const char *baseId = baseElem.queryProp("@id");
                    if (!baseId)
                        throw MakeStringException(0, "PACKAGE_ERROR: base element missing id attribute");
                    const IHpccPackage *base = packages->queryPackage(baseId);
                    if (!base)
                        throw MakeStringException(0, "PACKAGE_ERROR: base package %s not found", baseId);
                    appendBase(base);
                }
                while(baseIterator->next());
            }
        }
    }

    // Search this package and any bases for an element matching xpath1, then return iterator for its children that match xpath2
    IPropertyTreeIterator *lookupElements(const char *xpath1, const char *xpath2) const
    {
        IPropertyTree *parentNode = TYPE::node->queryPropTree(xpath1);
        if (parentNode)
            return parentNode->getElements(xpath2);
        ForEachItemIn(idx, bases)
        {
            const self &basePackage = bases.item(idx);
            IPropertyTreeIterator *it = basePackage.lookupElements(xpath1, xpath2);
            if (it)
                return it;
        }
        return NULL;
    }

    bool hasSuperFile(const char *superFileName) const
    {
        if (!superFileName || !*superFileName || !TYPE::node)
            return false;

        if (*superFileName=='~')
            superFileName++;
        StringBuffer xpath;
        if (TYPE::hasProp(TYPE::makeSuperFileXPath(xpath, superFileName)))
            return true;
        ForEachItemIn(idx, bases)
        {
            if (bases.item(idx).hasProp(xpath))
                return true;
        }

        return false;
    }
};


typedef CResolvedPackage<CPackageNode> CHpccPackage;

//================================================================================================
// CPackageMap - an implementation of IPackageMap using a string map
//================================================================================================

template <class TYPE, class IFACE>
class CPackageMapOf : public CInterface, implements IHpccPackageMap
{
public:
    typedef CResolvedPackage<TYPE> packageType;
    MapStringToMyClassViaBase<packageType, IFACE> packages;
    StringAttr packageId;
    StringAttr querySet;
    bool active;
    StringArray wildMatches, wildIds;
public:
    IMPLEMENT_IINTERFACE;
    CPackageMapOf(const char *_packageId, const char *_querySet, bool _active)
        : packageId(_packageId), querySet(_querySet), active(_active), packages(true)
    {
    }

    // IPackageMap interface
    virtual bool isActive() const
    {
        return active;
    }

    virtual const packageType *queryResolvedPackage(const char *name) const
    {
        return name ? packages.getValue(name) : NULL;
    }

    virtual const IHpccPackage *queryPackage(const char *name) const
    {
        return name ? (IFACE*)packages.getValue(name) : NULL;
    }

    virtual const char *queryPackageId() const
    {
        return packageId;
    }

    virtual const packageType *matchResolvedPackage(const char *name) const
    {
        if (name)
        {
            const packageType *pkg = queryResolvedPackage(name);
            if (pkg)
                return pkg;
            ForEachItemIn(idx, wildMatches)
            {
                if (WildMatch(name, wildMatches.item(idx), true))
                    return queryResolvedPackage(wildIds.item(idx));
            }
        }
        return NULL;
    }

    const IHpccPackage *matchPackage(const char *name) const
    {
        return (IFACE *) matchResolvedPackage(name);
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
            Owned<packageType> package = new packageType(&packageTree);
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
            packageType *pkg = packages.getValue((const char *)it.query().getKey());
            if (pkg)
                pkg->resolveBases(this);
        }
    }

    void load(const char *id)
    {
        load(getPackageMapById(id, true));
    }
};

typedef CPackageMapOf<CPackageNode, IHpccPackage> CHpccPackageMap;


//================================================================================================
// CHpccPackageSet - an implementation of IHpccPackageSet
//================================================================================================

class WORKUNIT_API CHpccPackageSet : public CInterface, implements IHpccPackageSet
{
    IArrayOf<CHpccPackageMap> packageMaps;
    StringAttr process;
public:
    IMPLEMENT_IINTERFACE;
    CHpccPackageSet(const char *_process);

    void load(IPropertyTree *xml);

    virtual const IHpccPackageMap *queryActiveMap(const char *queryset) const;
};

#endif
