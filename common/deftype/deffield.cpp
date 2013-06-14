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

#include "jlib.hpp"
#include "jstream.hpp"
#include "jmisc.hpp"
#include "defvalue.hpp"
#include "jexcept.hpp"
#include "jlzw.hpp"

#include "deffield.ipp"

//------------------------------------------------------------------------------------------------

CDefRecordElement::CDefRecordElement(DefElemKind _kind, IAtom * _name, ITypeInfo * _type, size32_t _maxSize)
{
    kind = _kind;
    name = _name;
    type.set(_type);
    maxSize = _maxSize;
    closed = false;
}

bool CDefRecordElement::operator==(IDefRecordElement const & other) const
{
    if((kind != other.getKind()) || (name != other.queryName()) || (maxSize != other.getMaxSize()) || (queryCompareValue() != other.queryCompareValue()) || (children.ordinality() != other.numChildren()))
            return false;
    if(type)
    {
        if((!other.queryType()) || !isSameBasicType(type, other.queryType()))
            return false;
    }
    else
    {
        if(other.queryType())
            return false;
    }
    ForEachItemIn(idx, children)
        if(!(children.item(idx) == *other.queryChild(idx)))
            return false;
    return true;
}

void CDefRecordElement::appendChild(IDefRecordElement * elem)
{
    assertex(!closed);
    CDefRecordElement * cast = dynamic_cast<CDefRecordElement *>(elem);
    children.append(*LINK(cast));
}

IDefRecordElement * CDefRecordElement::close()
{
    assertex(!closed);
    closed = true;
    return this;
}

//------------------------------------------------------------------------------------------------

CDefRecordBuilder::CDefRecordBuilder(unsigned maxSize)
{
    Owned<ITypeInfo> type = makeRecordType();
    elem.setown(new CDefRecordElement(DEKrecord, NULL, type, maxSize));
}

void CDefRecordBuilder::addChild(IDefRecordElement * element)
{
    elem->appendChild(element);
}

void CDefRecordBuilder::addChildOwn(IDefRecordElement * element)
{
    elem->appendChild(element);
    element->Release();
}

IDefRecordElement * CDefRecordBuilder::close()
{
    return elem.getClear()->close();
}

//------------------------------------------------------------------------------------------------

CDefRecordMeta::CDefRecordMeta(IDefRecordElement * _record, unsigned _numKeyed) : record(_record), numKeyed(_numKeyed)
{
}

//------------------------------------------------------------------------------------------------

IDefRecordElement * createDEfield(IAtom * name, ITypeInfo * type, IDefRecordElement * record, size32_t maxSize)
{
    CDefRecordElement * elem = new CDefRecordElement(DEKfield, name, type, maxSize);
    if (record)
        elem->appendChild(record);
    return elem->close();
}

IDefRecordBuilder * createDErecord(size32_t maxSize)
{
    return new CDefRecordBuilder(maxSize);
}

IDefRecordElement * createDEifblock(IDefRecordElement * field, IValue * value, IDefRecordElement * record)
{
    CDefRecordElement * elem = new CDefIfBlock(value);
    elem->appendChild(field);
    elem->appendChild(record);
    return elem->close();
}

IDefRecordMeta * createDefRecordMeta(IDefRecordElement * record, unsigned numKeyed)
{
    return new CDefRecordMeta(record, numKeyed);
}

//------------------------------------------------------------------------------------------------

static void serializeElement(MemoryBuffer & target, IDefRecordElement * elem)
{
    byte kind = elem ? elem->getKind() : DEKnone;
    target.append(kind);
    switch (kind)
    {
    case DEKnone:
        break;
    case DEKrecord:
        {
            size32_t maxSize = elem->getMaxSize();
            unsigned numChildren = elem->numChildren();
            target.append(maxSize).append(numChildren);
            for (unsigned i=0; i < numChildren; i++)
                serializeElement(target, elem->queryChild(i));
            break;
        }
    case DEKifblock:
        {
            IValue * value = elem->queryCompareValue();
            serializeValue(target, value);
            serializeElement(target, elem->queryChild(0));
            serializeElement(target, elem->queryChild(1));
            break;
        }
    case DEKfield:
        {
            IAtom * name = elem->queryName();
            ITypeInfo * type = elem->queryType();
            size32_t maxSize = elem->getMaxSize();
            serializeAtom(target, name);
            type->serialize(target);
            serializeElement(target, elem->queryChild(0));
            target.append(maxSize);
            break;
        }
    default:
        throwUnexpected();
    }
}

static void doSerializeRecordMeta(MemoryBuffer & target, IDefRecordMeta * meta)
{
    serializeElement(target, meta->queryRecord());
    target.append(meta->numKeyedFields());
}

extern DEFTYPE_API void serializeRecordMeta(MemoryBuffer & target, IDefRecordMeta * meta, bool compress)
{
    if (compress)
    {
        MemoryBuffer temp;
        doSerializeRecordMeta(temp, meta);
        compressToBuffer(target, temp.length(), temp.toByteArray());
    }
    else
        doSerializeRecordMeta(target, meta);
}

static IDefRecordElement * deserializeElement(MemoryBuffer & source)
{
    byte kind;
    source.read(kind);
    switch (kind)
    {
    case DEKnone:
        return NULL;
    case DEKrecord:
        {
            size32_t maxSize;
            unsigned numChildren;
            source.read(maxSize).read(numChildren);
            Owned<IDefRecordBuilder> builder = createDErecord(maxSize);
            for (unsigned i=0; i < numChildren; i++)
                builder->addChildOwn(deserializeElement(source));
            return builder->close();
        }
    case DEKifblock:
        {
            Owned<IValue> value = deserializeValue(source);
            Owned<IDefRecordElement> field = deserializeElement(source);
            Owned<IDefRecordElement> record = deserializeElement(source);
            return createDEifblock(field, value, record);
        }
    case DEKfield:
        {
            IAtom * name = deserializeAtom(source);
            Owned<ITypeInfo> type = deserializeType(source);
            Owned<IDefRecordElement> record = deserializeElement(source);
            size32_t maxSize;
            source.read(maxSize);
            return createDEfield(name, type, record, maxSize);
        }
    default:
        throwUnexpected();
    }
}

static IDefRecordMeta * doDeserializeRecordMeta(MemoryBuffer & source)
{
    Owned<IDefRecordElement> record = deserializeElement(source);
    unsigned numKeyed;
    source.read(numKeyed);
    return createDefRecordMeta(record, numKeyed);
}

extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(MemoryBuffer & source, bool compress)
{
    if (compress)
    {
        MemoryBuffer temp;
        decompressToBuffer(temp, source);
        return doDeserializeRecordMeta(temp);
    }
    else
        return doDeserializeRecordMeta(source);
}

extern DEFTYPE_API void serializeRecordMeta(IPropertyTree * target, IDefRecordMeta * meta);
extern DEFTYPE_API IDefRecordMeta * deserializeRecordMeta(const IPropertyTree * source);

static void addElementToPTree(IPropertyTree * root, IDefRecordElement * elem)
{
    byte kind = elem ? elem->getKind() : DEKnone;
    Owned<IPTree> branch = createPTree();
    StringAttr branchName;
    switch (kind)
    {
    case DEKnone:
        branchName.set("None");
        assertex(elem->numChildren() == 0);
        break;
    case DEKrecord:
        {
            branchName.set("Record");
            branch->setPropInt("@maxSize", elem->getMaxSize());
            unsigned numChildren = elem->numChildren();
            for (unsigned i=0; i < numChildren; i++)
                addElementToPTree(branch, elem->queryChild(i));
            break;
        }
    case DEKifblock:
        {
            branchName.set("IfBlock");
            StringBuffer value;
            elem->queryCompareValue()->getStringValue(value);
            branch->setProp("@compareValue", value.str());
            assertex(elem->numChildren() == 2);
            addElementToPTree(branch, elem->queryChild(0));
            addElementToPTree(branch, elem->queryChild(0));
            break;
        }
    case DEKfield:
        {
            branchName.set("Field");
            branch->setProp("@name", elem->queryName()->str());
            branch->setPropInt("@maxSize", elem->getMaxSize());
            StringBuffer type;
            elem->queryType()->getDescriptiveType(type);
            branch->setProp("@type", type.str());
            assertex(elem->numChildren() <= 1);
            if(elem->numChildren())
                addElementToPTree(branch, elem->queryChild(0));
            break;
        }
    default:
        throwUnexpected();
    }
    root->addPropTree(branchName.get(), branch.getClear());
}

extern DEFTYPE_API StringBuffer & getRecordMetaAsString(StringBuffer & out, IDefRecordMeta const * meta)
{
    Owned<IPropertyTree> tree = createPTree("RecordMeta");
    tree->setPropInt("@numKeyedFields", meta->numKeyedFields());
    addElementToPTree(tree, meta->queryRecord());
    toXML(tree, out);
    return out;
}
