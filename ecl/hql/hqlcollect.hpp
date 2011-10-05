/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    virtual _ATOM queryEclName() const = 0;
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
    virtual IEclSource * getSource(IEclSource * optParent, _ATOM eclName) = 0;
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
extern HQL_API IEclSourceCollection * createSingleDefinitionEclCollection(const char * moduleName, const char * attrName, const char * text);
extern HQL_API IEclSourceCollection * createRemoteXmlEclCollection(IEclUser * user, IXmlEclRepository & repository, const char * snapshot, bool useSandbox);

extern HQL_API IXmlEclRepository * createArchiveXmlEclRepository(IPropertyTree * archive);
extern HQL_API IXmlEclRepository * createReplayXmlEclRepository(IPropertyTree * xml);

#endif
