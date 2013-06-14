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

#ifndef _DEFFIELD_IPP
#define _DEFFIELD_IPP

#include "deffield.hpp"


class CDefRecordElement : public CInterface, implements IDefRecordElement
{
public:
    CDefRecordElement(DefElemKind _kind, IAtom * _name, ITypeInfo * _type, size32_t _maxSize = 0);
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
    virtual IAtom * queryName() const
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
    IAtom * name;
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
