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

#ifndef _HQLCOLLECT_HPP_
#define _HQLCOLLECT_HPP_

#include "hql.hpp"

typedef unsigned __int64 timestamp_t;

//An opaque type which can be linked.
interface IEclUser : public IInterface
{
};

interface IXmlEclRepository : public IInterface
{
public:
    virtual int getModules(StringBuffer & xml, IEclUser * user, timestamp_t timestamp) = 0;
    virtual int getAttributes(StringBuffer & xml, IEclUser * user, const char * modname, const char * attribute, int version, unsigned char infoLevel, const char * snapshot, bool sandbox4snapshot) = 0;
};


//---------------------------------------------------------------------------------------------------------------------

enum EclSourceType { ESTnone,
                     ESTdefinition, ESTmodule,
                     ESTlibrary, ESTplugin,
                     ESTcontainer = 0x10 };

interface IProperties;
interface IFileContents;
interface IEclSource : public IInterface
{
    virtual IFileContents * queryFileContents() = 0;
    virtual IProperties * getProperties() = 0;
    virtual IIdAtom * queryEclId() const = 0;
    virtual EclSourceType queryType() const = 0;

    inline bool isImplicitModule() const
    {
        EclSourceType type = queryType();
        return (type==ESTlibrary) || (type==ESTplugin);
    }
    //virtual bool isPluginDllScope(IHqlScope * scope)
    //virtual bool allowImplicitImport(IHqlScope * scope)
};

typedef IIteratorOf<IEclSource> IEclSourceIterator;
interface IEclSourceCollection : public IInterface
{
    virtual IEclSource * getSource(IEclSource * optParent, IIdAtom * eclName) = 0;
    virtual IEclSourceIterator * getContained(IEclSource * optParent) = 0;
    virtual void checkCacheValid() = 0;
};

typedef IArrayOf<IEclSourceCollection> EclSourceCollectionArray;

//---------------------------------------------------------------------------------------------------------------------

enum EclSourceCollectionFlags {
    ESFnone = 0,
    ESFallowplugins = 0x0001,
};

extern HQL_API IEclSourceCollection * createFileSystemEclCollection(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace);
extern HQL_API IEclSourceCollection * createArchiveEclCollection(IPropertyTree * tree);
extern HQL_API IEclSourceCollection * createSingleDefinitionEclCollection(const char * moduleName, const char * attrName, IFileContents * contents);
extern HQL_API IEclSourceCollection * createSingleDefinitionEclCollection(const char * attrName, IFileContents * contents);
extern HQL_API IEclSourceCollection * createRemoteXmlEclCollection(IEclUser * user, IXmlEclRepository & repository, const char * snapshot, bool useSandbox);

extern HQL_API IXmlEclRepository * createArchiveXmlEclRepository(IPropertyTree * archive);
extern HQL_API IXmlEclRepository * createReplayXmlEclRepository(IPropertyTree * xml);

#endif
