/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jlib.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtldynfield.hpp"

//---------------------------------------------------------------------------------------------------------------------

const RtlTypeInfo *FieldTypeInfoStruct::createRtlTypeInfo() const
{
    const RtlTypeInfo *ret = nullptr;
    switch (fieldType & RFTMkind)
    {
    case type_boolean:
        ret = new RtlBoolTypeInfo(fieldType, length);
        break;
    case type_int:
        ret = new RtlIntTypeInfo(fieldType, length);
        break;
    case type_real:
        ret = new RtlRealTypeInfo(fieldType, length);
        break;
    case type_decimal:
        ret = new RtlDecimalTypeInfo(fieldType, length);
        break;
    case type_string:
        ret = new RtlStringTypeInfo(fieldType, length);
        break;
    case type_bitfield:
        ret = new RtlBitfieldTypeInfo(fieldType, length);
        break;
    case type_varstring:
        ret = new RtlVarStringTypeInfo(fieldType, length);
        break;
    case type_data:
        ret = new RtlDataTypeInfo(fieldType, length);
        break;
    case type_table:
        assert(childType);
        ret = new RtlDatasetTypeInfo(fieldType, length, childType);
        break;
    case type_set:
        assert(childType);
        ret = new RtlSetTypeInfo(fieldType, length, childType);
        break;
    case type_row:
        assert(childType);
        ret = new RtlRowTypeInfo(fieldType, length, childType);
        break;
    case type_swapint:
        ret = new RtlSwapIntTypeInfo(fieldType, length);
        break;
    case type_packedint:
        ret = new RtlPackedIntTypeInfo(fieldType, length);
        break;
    case type_qstring:
        ret = new RtlQStringTypeInfo(fieldType, length);
        break;
    case type_unicode:
        ret = new RtlUnicodeTypeInfo(fieldType, length, locale);
        break;
    case type_varunicode:
        ret = new RtlVarUnicodeTypeInfo(fieldType, length, locale);
        break;
    case type_utf8:
        ret = new RtlUtf8TypeInfo(fieldType, length, locale);
        break;
    case type_record:
        ret = new RtlRecordTypeInfo(fieldType, length, fieldsArray);
        break;
    default:
        throwUnexpected();
    }
    return ret;
};

typedef MapBetween<const RtlTypeInfo *, const RtlTypeInfo *, StringAttr, const char *> TypeNameMap;

/**
 * class CRtlFieldTypeSerializer
 *
 * Serializer class for creating json representation of a RtlTypeInfo structure.
 *
 */

class CRtlFieldTypeSerializer
{
public:
    /**
     * Serialize a RtlTypeInfo structure to JSON
     *
     * @param  out  Buffer for resulting serialized string
     * @param  type RtlTypeInfo structure to be serialized
     * @return Referenced to supplied buffer
     */
     static StringBuffer &serialize(StringBuffer &out, const RtlTypeInfo *type)
     {
         CRtlFieldTypeSerializer s(out, type);
         s.doSerialize();
         return out;
     }
private:
    CRtlFieldTypeSerializer(StringBuffer &_out, const RtlTypeInfo *_base)
    : json(_out), base(_base)
    {
    }
    void doSerialize()
    {
        json.append("{");
        serializeType(base);
        json.append("\n}");
    }
    void serializeType(const RtlTypeInfo *type)
    {
        if (!serialized(type))
        {
            // Make sure all child types are serialized first
            const RtlTypeInfo *child = type->queryChildType();
            if (child)
                serializeType(child);
            const RtlFieldInfo * const * fields = type->queryFields();
            if (fields)
            {
                for (;;)
                {
                    const RtlFieldInfo * child = *fields;
                    if (!child)
                        break;
                    serializeType(child->type);
                    fields++;
                }
            }
            // Now serialize this one
            if (type != base)
            {
                VStringBuffer newName("ty%d", ++nextTypeName);
                types.setValue(type, newName.str());
                startField(newName.str());
                serializeMe(type);
                closeCurly();
            }
            else
                serializeMe(type);
        }
    }

    void serializeMe(const RtlTypeInfo *type)
    {
        if (!type->canSerialize())
            throw makeStringException(MSGAUD_user, 1000, "IFBLOCK and DICTIONARY type structures cannot be serialized");
        addPropHex("fieldType", type->fieldType);
        addProp("length", type->length);
        addPropNonEmpty("locale", type->queryLocale());
        const RtlTypeInfo *child = type->queryChildType();
        if (child)
            addPropType("child", child);
        const RtlFieldInfo * const * fields = type->queryFields();
        if (fields)
        {
            startFields();
            for (;;)
            {
                const RtlFieldInfo * child = *fields;
                if (!child)
                    break;
                newline();
                openCurly();
                addProp("name", child->name);
                addPropType("type", child->type);
                addProp("xpath", child->xpath);
                if (child->flags)
                    addPropHex("flags", child->flags);
                // initializer is tricky - it's not (in general) a null-terminated string but the actual length is not easily available
                if (child->initializer)
                {
                    addProp("init", child->type->size(child->initializer, nullptr), child->initializer);
                }
                closeCurly();
                fields++;
            }
            endFields();
        }
    }
    bool serialized(const RtlTypeInfo *type)
    {
        return types.find(type) != nullptr;
    }
    void startField(const char *name)
    {
        newline().appendf("\"%s\": ", name);
        openCurly();
    }
    void addProp(const char *propName, const char *propVal)
    {
        if (propVal)
        {
            newline();
            encodeJSON(json.append("\""), propName).append("\": ");
            encodeJSON(json.append("\""), propVal).append("\"");
        }
    }
    void addProp(const char *propName, size32_t propLen, const byte *propVal)
    {
        if (propVal)
        {
            newline();
            encodeJSON(json.append("\""), propName).append("\": \"");
            JBASE64_Encode(propVal, propLen, json, false);
            json.append("\"");
        }
    }
    void addPropNonEmpty(const char *propName, const char *propVal)
    {
        if (propVal && *propVal)
            addProp(propName, propVal);
    }
    void addProp(const char *propName, unsigned propVal)
    {
        newline().appendf("\"%s\": %u", propName, propVal);
    }
    void addPropHex(const char *propName, unsigned propVal)
    {
        newline().appendf("\"%s\": %u", propName, propVal);  // Nice idea but json does not support hex constants :(
    }
    void addPropType(const char *propName, const RtlTypeInfo *type)
    {
        addProp(propName, queryTypeName(type));
    }
    const char *queryTypeName(const RtlTypeInfo *type)
    {
        StringAttr *typeName = types.getValue(type);
        assertex(typeName);
        return typeName->get();
    }
    void startFields()
    {
        newline().appendf("\"fields\": ");
        openCurly('[');
    }
    void endFields()
    {
        closeCurly(']');
    }
    StringBuffer &newline()
    {
        if (commaPending)
            json.append(',');
        json.appendf("\n%*s", indent, "");
        commaPending = true;
        return json;
    }
    void closeCurly(char brace = '}')
    {
        indent--;
        json.appendf("\n%*s%c", indent, "", brace);
        commaPending = true;
    }
    void openCurly(char brace = '{')
    {
        json.append(brace);
        indent++;
        commaPending = false;
    }

    TypeNameMap types;
    StringBuffer &json;
    const RtlTypeInfo *base = nullptr;
    unsigned indent = 1;
    unsigned nextTypeName = 0;
    bool commaPending = false;
};

/**
 * class CRtlFieldTypeDeserializer
 *
 * Deserializer class for creating a RtlTypeInfo structure from json representation.
 *
 * Note that the resulting RtlTypeInfo structures are owned by this object and will be
 * destroyed when this object is destroyed.
 *
 */

class CRtlFieldTypeDeserializer : public CInterfaceOf<IRtlFieldTypeDeserializer>
{
public:
    /**
     * CRtlFieldTypeDeserializer constructor
     *
     */
    CRtlFieldTypeDeserializer()
    {
    }
    /**
     * CRtlFieldTypeDeserializer destructor
     * <p>
     * Releases all RtlTypeInfo and related structures created by this deserializer
     */
    ~CRtlFieldTypeDeserializer()
    {
        // Need some care - all the RtlTypeInfo objects I created need to be destroyed, together with anything else I had to create
        // Strings (other than the init strings) are preserved in the AtomTable
        HashIterator allTypes(types);
        ForEach(allTypes)
        {
            const RtlTypeInfo **type = types.mapToValue(&allTypes.query());
            cleanupType(*type);
        }
        cleanupType(base);
    }
    /**
     * Obtain the deserialized type information
     * <p>
     * Note that the RtlTypeInfo objects are not link-counted, so the lifetime of these objects
     * is determined by the lifetime of the deserializer. They will be released once the deserializer
     * that created them is deleted.
     * <p>
     * Do not call more than once.
     *
     * @param  _json JSON text to be deserialized, as created by CRtlFieldTypeSerializer
     * @return Deserialized type object
     */
    virtual const RtlTypeInfo *deserialize(const char *json) override
    {
        assertex(!base);
        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(json);
        base = deserializeType(jsonTree, jsonTree);
        return base;
    }

    virtual const RtlTypeInfo *addType(FieldTypeInfoStruct &info, const ITypeInfo *type) override
    {
        VStringBuffer name("%p", type);
        const RtlTypeInfo ** found = types.getValue(name);
        if (found)
            return *found;
        info.locale = keep(info.locale);
        const RtlTypeInfo * ret = info.createRtlTypeInfo(); // MORE - need to add to types to ensure freed - also would be nice to dedup?
        types.setValue(name, ret);
        return ret;
    }

    virtual const RtlTypeInfo *lookupType(const ITypeInfo *type) const override
    {
        VStringBuffer name("%p", type);
        const RtlTypeInfo ** found = types.getValue(name);
        if (found)
            return *found;
        return nullptr;
    }

    virtual const RtlFieldInfo *addFieldInfo(const char *fieldName, const char *xpath, const RtlTypeInfo *type, unsigned flags, const char *init) override
    {
        // MORE - we could hang onto this for cleanup, rather than assuming that we keep it via a later addType() call?
        return new RtlFieldStrInfo(keep(fieldName), keep(xpath), type, flags, init);
    }

private:
    KeptAtomTable atoms;     // Used to ensure proper lifetime of strings used in type structures
    MapStringTo<const RtlTypeInfo *> types;  // Ensures structures only generated once
    const RtlTypeInfo *base = nullptr;       // Holds the resulting type

    void cleanupType(const RtlTypeInfo *type)
    {
        if (type)
        {
            // Releases all memory for a single RtlTypeInfo object
            const RtlFieldInfo * const * fields = type->queryFields();
            if (fields)
            {
                const RtlFieldInfo * const * cur = fields;
                for (;;)
                {
                    const RtlFieldInfo * child = *cur;
                    if (!child)
                        break;
                    // We don't need to delete other strings - they are owned by atom table.
                    // But the initializer is decoded and thus owned by me
                    delete child->initializer;
                    delete child;
                    cur++;
                }
                delete [] fields;
            }
            delete type;
        }
    }
    const RtlTypeInfo *lookupType(const char *name, IPropertyTree *all)
    {
        const RtlTypeInfo ** found = types.getValue(name);
        if (found)
            return *found;
        const RtlTypeInfo *type = deserializeType(all->queryPropTree(name), all);
        types.setValue(name, type);
        return type;
    }
    const char *keep(const char *string)
    {
        if (string)
            return str(atoms.addAtom(string));
        else
            return nullptr;
    }
    const RtlTypeInfo *deserializeType(IPropertyTree *type, IPropertyTree *all)
    {
        FieldTypeInfoStruct info;
        info.fieldType = type->getPropInt("fieldType");
        info.length = type->getPropInt("length");
        info.locale = keep(type->queryProp("locale"));
        const char *child = type->queryProp("child");
        if (child)
            info.childType = lookupType(child, all);
        if ((info.fieldType & RFTMkind) == type_record)
        {
            unsigned numFields = type->getCount("fields");
            info.fieldsArray = new const RtlFieldInfo * [numFields+1];
            info.fieldsArray[numFields] = nullptr;
            Owned<IPropertyTreeIterator> fields = type->getElements("fields");
            unsigned n = 0;
            ForEach(*fields)
            {
                IPropertyTree &field = fields->query();
                const char *fieldTypeName = field.queryProp("type");
                const char *fieldName = keep(field.queryProp("name"));
                const char *fieldXpath = keep(field.queryProp("xpath"));
                unsigned flags = field.getPropInt("flags");
                const char *fieldInit = field.queryProp("init");
                if (fieldInit)
                {
                    StringBuffer decoded;
                    JBASE64_Decode(fieldInit, decoded);
                    fieldInit = decoded.detach(); // NOTE - this gets freed in cleanupType()
                }
                info.fieldsArray[n] = new RtlFieldStrInfo(fieldName, fieldXpath, lookupType(fieldTypeName, all), flags, fieldInit);
                n++;
            }
        }
        return info.createRtlTypeInfo();
    }
};

extern ECLRTL_API IRtlFieldTypeDeserializer *createRtlFieldTypeDeserializer()
{
    return new CRtlFieldTypeDeserializer;
}


extern ECLRTL_API void dumpDatasetType(size32_t & __lenResult,char * & __result,IOutputMetaData &  metaVal,IRowStream * val)
{
    StringBuffer ret;
    CRtlFieldTypeSerializer::serialize(ret, metaVal.queryTypeInfo());

#ifdef _DEBUG
    CRtlFieldTypeDeserializer deserializer;
    StringBuffer ret2;
    CRtlFieldTypeSerializer::serialize(ret2, deserializer.deserialize(ret));
    assert(streq(ret, ret2));
#endif

    __lenResult = ret.length();
    __result = ret.detach();
}

extern ECLRTL_API StringBuffer &dumpTypeInfo(StringBuffer &ret, const RtlTypeInfo *t)
{
    return CRtlFieldTypeSerializer::serialize(ret, t);
}

extern ECLRTL_API void dumpRecordType(size32_t & __lenResult,char * & __result,IOutputMetaData &  metaVal,const byte * val)
{
    StringBuffer ret;
    CRtlFieldTypeSerializer::serialize(ret, metaVal.queryTypeInfo());

#ifdef _DEBUG
    CRtlFieldTypeDeserializer deserializer;
    StringBuffer ret2;
    CRtlFieldTypeSerializer::serialize(ret2, deserializer.deserialize(ret));
    assert(streq(ret, ret2));
#endif

    __lenResult = ret.length();
    __result = ret.detach();
}
