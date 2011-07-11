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
    virtual _ATOM queryName() const = 0;
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

extern DEFTYPE_API IDefRecordElement * createDEfield(_ATOM name, ITypeInfo * type, IDefRecordElement * record, size32_t maxSize=0);
extern DEFTYPE_API IDefRecordBuilder * createDErecord(size32_t maxSize);
extern DEFTYPE_API IDefRecordElement * createDEifblock(IDefRecordElement * field, IValue * value, IDefRecordElement * record);
extern DEFTYPE_API IDefRecordMeta * createDefRecordMeta(IDefRecordElement * record, unsigned numKeyed);

extern DEFTYPE_API void serializeRecordMeta(MemoryBuffer & target, IDefRecordMeta * meta, bool compress);
extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(MemoryBuffer & source, bool compress);
extern DEFTYPE_API void serializeRecordMeta(IPropertyTree * target, IDefRecordMeta * meta);
extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(const IPropertyTree * source);

extern DEFTYPE_API StringBuffer & getRecordMetaAsString(StringBuffer & out, IDefRecordMeta const * meta);

#endif
