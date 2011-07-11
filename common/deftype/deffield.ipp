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

#ifndef _DEFFIELD_IPP
#define _DEFFIELD_IPP

#include "deffield.hpp"


class CDefRecordElement : public CInterface, implements IDefRecordElement
{
public:
    CDefRecordElement(DefElemKind _kind, _ATOM _name, ITypeInfo * _type, size32_t _maxSize = 0);
    IMPLEMENT_IINTERFACE

    virtual DefElemKind getKind() const
    {
        return (DefElemKind)kind;
    }
    virtual size32_t getMaxSize() const
    {
        return maxSize;
    }
    virtual ITypeInfo *queryType() const
    {
        return type;
    }
    virtual _ATOM queryName() const
    {
        return name;
    }
    virtual IValue * queryCompareValue() const
    {
        return NULL;
    }
    virtual unsigned numChildren() const
    {
        return children.ordinality();
    }
    virtual IDefRecordElement * queryChild(unsigned i) const
    {
        if (children.isItem(i))
            return &children.item(i);
        return NULL;
    }
    virtual bool operator==(IDefRecordElement const & other) const;

//internal public interface
    virtual void appendChild(IDefRecordElement * elem);
    virtual IDefRecordElement * close();

protected:
    byte kind;
    _ATOM name;
    Owned<ITypeInfo> type;
    CIArrayOf<CDefRecordElement> children;
    size32_t maxSize;
    bool closed;
};

class CDefIfBlock : public CDefRecordElement
{
public:
    CDefIfBlock(IValue * _value) : CDefRecordElement(DEKifblock, NULL, NULL)
    {
        value.set(_value);
    }
    
    virtual IValue * queryCompareValue() const
    {
        return value;
    }

protected:
    Owned<IValue> value;
};


class CDefRecordMeta : public CInterface, implements IDefRecordMeta
{
public:
    CDefRecordMeta(IDefRecordElement * _record, unsigned _numKeyed);
    IMPLEMENT_IINTERFACE

    virtual IDefRecordElement * queryRecord() const { return record; }
    virtual unsigned numKeyedFields() const { return numKeyed; }
    virtual bool equals(const IDefRecordMeta *other) const
    {
        return other && numKeyed==other->numKeyedFields() && *record==*other->queryRecord();
    }
    virtual bool IsShared() const { return CInterface::IsShared(); }

protected:
    Linked<IDefRecordElement> record;
    unsigned numKeyed;
};

class CDefRecordBuilder : public CInterface, implements IDefRecordBuilder
{
public:
    CDefRecordBuilder(unsigned maxSize);
    IMPLEMENT_IINTERFACE

    virtual void addChild(IDefRecordElement * element);
    virtual void addChildOwn(IDefRecordElement * element);
    virtual IDefRecordElement * close();

protected:
    Owned<CDefRecordElement> elem;
};




#endif
