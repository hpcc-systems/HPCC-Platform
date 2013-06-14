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

#ifndef _DEFFIELD_HPP
#define _DEFFIELD_HPP

#include "jptree.hpp"
#include "deftype.hpp"
#include "defvalue.hpp"

enum DefElemKind { DEKnone, DEKrecord, DEKifblock, DEKfield, DEKattr };

interface IDefRecordElement : public IInterface
{
    virtual DefElemKind getKind() const = 0;
    virtual size32_t getMaxSize() const = 0;
    virtual unsigned numChildren() const = 0;
    virtual ITypeInfo *queryType() const = 0;
    virtual IAtom * queryName() const = 0;
    virtual IDefRecordElement * queryChild(unsigned i) const = 0;
    virtual IValue * queryCompareValue() const = 0;
    virtual bool operator==(IDefRecordElement const & other) const = 0;
};
typedef IArrayOf<IDefRecordElement> IDefRecordElementArray;

//fields of type record/dataset have queryChild(0) as the dataset
//ifblocks have queryChild(0) as the condition field, queryChild(1) as the child record
//unnamed records (used for inheritance) are already expanded inline 
//records are not commoned up - even if the same structure is used with two names.  This ensures each field has a unique IDefRecordElement
//Whether a field needs biasing is determined by the numKeyedFields() and the number of root fields

interface IDefRecordMeta : public IInterface
{
    virtual IDefRecordElement * queryRecord() const = 0;
    virtual unsigned numKeyedFields() const = 0;
    virtual bool equals(const IDefRecordMeta *other) const = 0;
    virtual bool IsShared() const = 0;
};

//Interfaces used by compiler to generate the meta information.
interface IDefRecordBuilder : public IInterface
{
    virtual void addChild(IDefRecordElement * element) = 0;
    virtual void addChildOwn(IDefRecordElement * element) = 0;
    virtual IDefRecordElement * close() = 0;
};

extern DEFTYPE_API IDefRecordElement * createDEfield(IAtom * name, ITypeInfo * type, IDefRecordElement * record, size32_t maxSize=0);
extern DEFTYPE_API IDefRecordBuilder * createDErecord(size32_t maxSize);
extern DEFTYPE_API IDefRecordElement * createDEifblock(IDefRecordElement * field, IValue * value, IDefRecordElement * record);
extern DEFTYPE_API IDefRecordMeta * createDefRecordMeta(IDefRecordElement * record, unsigned numKeyed);

extern DEFTYPE_API void serializeRecordMeta(MemoryBuffer & target, IDefRecordMeta * meta, bool compress);
extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(MemoryBuffer & source, bool compress);
extern DEFTYPE_API void serializeRecordMeta(IPropertyTree * target, IDefRecordMeta * meta);
extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(const IPropertyTree * source);

extern DEFTYPE_API StringBuffer & getRecordMetaAsString(StringBuffer & out, IDefRecordMeta const * meta);

#endif
