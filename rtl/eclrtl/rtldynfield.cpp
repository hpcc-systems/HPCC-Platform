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
#include "rtlrecord.hpp"
#include "rtlembed.hpp"

//#define TRACE_TRANSLATION

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
typedef MapBetween<const RtlTypeInfo *, const RtlTypeInfo *, unsigned, unsigned> TypeNumMap;

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

class CRtlFieldTypeBinSerializer
{
public:
    /**
     * Serialize a RtlTypeInfo structure to binary
     *
     * @param  out  Buffer for resulting serialized string
     * @param  type RtlTypeInfo structure to be serialized
     * @return Referenced to supplied buffer
     */
     static MemoryBuffer &serialize(MemoryBuffer &out, const RtlTypeInfo *type)
     {
         CRtlFieldTypeBinSerializer s(out);
         s.serializeType(type);
         return out;
     }
private:
    CRtlFieldTypeBinSerializer(MemoryBuffer &_out)
    : out(_out)
    {
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
            types.setValue(type, nextTypeNum++);
            serializeMe(type);
        }
    }

    void serializeMe(const RtlTypeInfo *type)
    {
        if (!type->canSerialize())
            throw makeStringException(MSGAUD_user, 1000, "IFBLOCK and DICTIONARY type structures cannot be serialized");
        unsigned fieldType = type->fieldType;
        const char *locale = type->queryLocale();
        if (locale && *locale)
            fieldType |= RFTMhasLocale;
        const RtlTypeInfo *child = type->queryChildType();
        if (child)
            fieldType |= RFTMhasChildType;
        const RtlFieldInfo * const * fields = type->queryFields();
        if (fields)
            fieldType |= RFTMhasFields;
        out.append(fieldType);
        out.append(type->length);
        if (fieldType & RFTMhasLocale)
            out.append(locale);
        if (child)
            out.appendPacked(queryTypeIdx(child));
        if (fields)
        {
            unsigned count = countFields(fields);
            out.appendPacked(count);
            for (;;)
            {
                const RtlFieldInfo * child = *fields;
                if (!child)
                    break;
                out.append(child->name);
                out.appendPacked(queryTypeIdx(child->type));
                unsigned flags = child->flags;
                if (child->xpath)
                    flags |= RFTMhasXpath;
                if (child->initializer)
                    flags |= RFTMhasInitializer;
                out.append(flags);
                if (child->xpath)
                    out.append(child->xpath);
                // initializer is tricky - it's not (in general) a null-terminated string but the actual length is not easily available
                if (child->initializer)
                {
                    unsigned initLength = child->type->size(child->initializer, nullptr);
                    out.appendPacked(initLength).append(initLength, child->initializer);
                }
                fields++;
            }
        }
    }
    bool serialized(const RtlTypeInfo *type)
    {
        return types.find(type) != nullptr;
    }
    unsigned queryTypeIdx(const RtlTypeInfo *type)
    {
        unsigned *typeNum = types.getValue(type);
        assertex(typeNum);
        return *typeNum;
    }

    TypeNumMap types;
    MemoryBuffer &out;
    unsigned nextTypeNum = 0;
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
    virtual const RtlTypeInfo *deserialize(MemoryBuffer &buf) override
    {
        assertex(!base);
        unsigned nextTypeNum = 0;
        while (buf.remaining())
        {
            if (base)
            {
                addType(base, nextTypeNum++);
                base = nullptr;  // in case of exceptions...
            }
            base = deserializeType(buf);
        }
        return base;
    }

    virtual const RtlTypeInfo *addType(FieldTypeInfoStruct &info, const ITypeInfo *type) override
    {
        VStringBuffer name("%p", type);
        const RtlTypeInfo ** found = types.getValue(name);
        if (found)
            return *found;
        info.locale = keep(info.locale);
        const RtlTypeInfo * ret = info.createRtlTypeInfo();
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
    const RtlTypeInfo *lookupType(unsigned idx)
    {
        // Could keep an expanding array of types instead - but the hash table is already there for json support...
        VStringBuffer key("%u", idx);
        const RtlTypeInfo ** found = types.getValue(key);
        if (found)
            return *found;
        throw makeStringException(-1, "Invalid serialized type information");
    }
    void addType(const RtlTypeInfo *type, unsigned idx)
    {
        VStringBuffer key("%u", idx);
        assert(types.getValue(key)==nullptr);
        types.setValue(key, type);
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

    const RtlTypeInfo *deserializeType(MemoryBuffer &type)
    {
        FieldTypeInfoStruct info;
        type.read(info.fieldType);
        type.read(info.length);
        if (info.fieldType & RFTMhasLocale)
        {
            const char *locale;
            type.read(locale);
            info.locale = keep(locale);
        }
        if (info.fieldType & RFTMhasChildType)
        {
            unsigned childIdx;
            type.readPacked(childIdx);
            info.childType = lookupType(childIdx);
        }
        if (info.fieldType & RFTMhasFields)
        {
            unsigned numFields;
            type.readPacked(numFields);
            info.fieldsArray = new const RtlFieldInfo * [numFields+1];
            info.fieldsArray[numFields] = nullptr;
            for (int n = 0; n < numFields; n++)
            {
                const char *fieldName;
                type.read(fieldName);
                unsigned fieldType;
                type.readPacked(fieldType);
                unsigned fieldFlags;
                type.read(fieldFlags);
                const char *xpath = nullptr;
                if (fieldFlags & RFTMhasXpath)
                    type.read(xpath);
                void *init = nullptr;
                if (fieldFlags & RFTMhasInitializer)
                {
                    unsigned initLength;
                    type.readPacked(initLength);
                    init = malloc(initLength);
                    memcpy(init, type.readDirect(initLength), initLength);
                }
                fieldFlags &= ~RFTMserializerFlags;
                info.fieldsArray[n] = new RtlFieldStrInfo(keep(fieldName), keep(xpath), lookupType(fieldType), fieldFlags, (const char *) init);
            }
        }
        info.fieldType &= ~RFTMserializerFlags;
        return info.createRtlTypeInfo();
    }
};

extern ECLRTL_API IRtlFieldTypeDeserializer *createRtlFieldTypeDeserializer()
{
    return new CRtlFieldTypeDeserializer;
}

extern ECLRTL_API StringBuffer &dumpTypeInfo(StringBuffer &ret, const RtlTypeInfo *t)
{
    return CRtlFieldTypeSerializer::serialize(ret, t);
}

extern ECLRTL_API void dumpRecordType(size32_t & __lenResult,char * & __result,IOutputMetaData &metaVal)
{
    StringBuffer ret;
    CRtlFieldTypeSerializer::serialize(ret, metaVal.queryTypeInfo());

#ifdef _DEBUG
    StringBuffer ret2;
    CRtlFieldTypeDeserializer deserializer;
    CRtlFieldTypeSerializer::serialize(ret2, deserializer.deserialize(ret));
    assert(streq(ret, ret2));
    MemoryBuffer out;
    CRtlFieldTypeBinSerializer::serialize(out, metaVal.queryTypeInfo());
    CRtlFieldTypeDeserializer bindeserializer;
    CRtlFieldTypeSerializer::serialize(ret2.clear(), bindeserializer.deserialize(out));
    assert(streq(ret, ret2));
#endif

    __lenResult = ret.length();
    __result = ret.detach();
}

extern ECLRTL_API void getFieldVal(size32_t & __lenResult,char * & __result, int column, IOutputMetaData &  metaVal, const byte *row)
{
    __lenResult = 0;
    __result = nullptr;
    if (column >= 0)
    {
        const RtlRecord &r = metaVal.queryRecordAccessor(true);
        unsigned numOffsets = r.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow offsetCalculator(r, row, numOffsets, variableOffsets);
        offsetCalculator.getUtf8(__lenResult, __result, column);
    }
}

extern ECLRTL_API int getFieldNum(const char *fieldName, IOutputMetaData &  metaVal)
{
    const RtlRecord r = metaVal.queryRecordAccessor(true);
    return r.getFieldNum(fieldName);
}
enum FieldMatchType {
    // On a field, exactly one of the above is set, but translator returns a bitmap indicating
    // which were required (and we can restrict translation to allow some types but not others)
    match_perfect     = 0x00,    // exact type match - use memcpy
    match_link        = 0x01,    // copy a nested dataset by linking
    match_move        = 0x02,    // at least one field has moved (set on translator)
    match_remove      = 0x04,    // at least one field has been removed (set on translator)
    match_truncate    = 0x08,    // dest is truncated copy of source - use memcpy
    match_extend      = 0x10,    // dest is padded version of source - use memcpy and memset
    match_typecast    = 0x20,    // type has changed - cast required
    match_none        = 0x40,    // No matching field in source - use null value
    match_recurse     = 0x80,    // Use recursive translator for child records/datasets
    match_fail        = 0x100,   // no translation possible
};

StringBuffer &describeFlags(StringBuffer &out, FieldMatchType flags)
{
    if (flags == match_perfect)
        return out.append("perfect");
    unsigned origlen = out.length();
    if (flags & match_link) out.append("|link");
    if (flags & match_move) out.append("|move");
    if (flags & match_remove) out.append("|remove");
    if (flags & match_truncate) out.append("|truncate");
    if (flags & match_extend) out.append("|extend");
    if (flags & match_typecast) out.append("|typecast");
    if (flags & match_none) out.append("|none");
    if (flags & match_recurse) out.append("|recurse");
    if (flags & match_fail) out.append("|fail");
    assertex(out.length() > origlen);
    return out.remove(origlen, 1);
}

inline constexpr FieldMatchType operator|(FieldMatchType a, FieldMatchType b) { return (FieldMatchType)((int)a | (int)b); }
inline FieldMatchType &operator|=(FieldMatchType &a, FieldMatchType b) { return (FieldMatchType &) ((int &)a |= (int)b); }

class GeneralRecordTranslator
{
public:
    GeneralRecordTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo)
    : destRecInfo(_destRecInfo), sourceRecInfo(_srcRecInfo)
    {
        matchInfo = new MatchInfo[destRecInfo.getNumFields()];
        createMatchInfo();
    }
    ~GeneralRecordTranslator()
    {
        delete [] matchInfo;
    }
    void describe(unsigned indent = 0) const
    {
        for (unsigned idx = 0; idx <  destRecInfo.getNumFields(); idx++)
        {
            const char *source = destRecInfo.queryName(idx);
            const MatchInfo &match = matchInfo[idx];
            if (match.matchType == match_none)
                DBGLOG("%*sNo match for field %s - default value will be used", indent, "", source);
            else
            {
                StringBuffer matchStr;
                DBGLOG("%*sMatch (%s) to field %d for field %s", indent, "", describeFlags(matchStr, match.matchType).str(), match.matchIdx, source);
                if (match.subTrans)
                    match.subTrans->describe(indent+2);
            }
        }
        if (!canTranslate())
            DBGLOG("%*sTranslation is NOT possible", indent, "");
        else if (needsTranslate())
        {
            StringBuffer matchStr;
            DBGLOG("%*sTranslation is possible (%s)", indent, "", describeFlags(matchStr, matchFlags).str());
        }
        else
            DBGLOG("%*sTranslation is not necessary", indent, "");
    }
    size32_t translate(ARowBuilder &builder, size32_t offset, const byte *sourceRec) const
    {
        unsigned numOffsets = sourceRecInfo.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow sourceRow(sourceRecInfo, sourceRec, numOffsets, variableOffsets);
        size32_t estimate = destRecInfo.getFixedSize();
        if (!estimate)
        {
            estimate = estimateNewSize(sourceRow);
            builder.ensureCapacity(offset+estimate, "record");
        }
        size32_t origOffset = offset;
        for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
        {
            const RtlFieldInfo *field = destRecInfo.queryField(idx);
            const RtlTypeInfo *type = field->type;
            const MatchInfo &match = matchInfo[idx];
            if (match.matchType == match_none || match.matchType==match_fail)
                offset = type->buildNull(builder, offset, field);
            else
            {
                unsigned matchField = match.matchIdx;
                const RtlTypeInfo *sourceType = sourceRecInfo.queryType(matchField);
                size_t sourceOffset = sourceRow.getOffset(matchField);
                const byte *source = sourceRec + sourceOffset;
                size_t copySize = sourceRow.getSize(matchField);
                switch (match.matchType)
                {
                case match_perfect:
                {
                    // Look ahead for other perfect matches and combine the copies
                    while (idx < destRecInfo.getNumFields()-1)
                    {
                        const MatchInfo &nextMatch = matchInfo[idx+1];
                        if (nextMatch.matchType == match_perfect && nextMatch.matchIdx == matchField+1)
                        {
                            idx++;
                            matchField++;
                        }
                        else
                            break;
                    }
                    size_t copySize = sourceRow.getOffset(matchField+1) - sourceOffset;
                    builder.ensureCapacity(offset+copySize, field->name);
                    memcpy(builder.getSelf()+offset, source, copySize);
                    offset += copySize;
                    break;
                }
                case match_truncate:
                {
                    assert(type->isFixedSize());
                    size32_t copySize = type->getMinSize();
                    builder.ensureCapacity(offset+copySize, field->name);
                    memcpy(builder.getSelf()+offset, source, copySize);
                    offset += copySize;
                    break;
                }
                case match_extend:
                {
                    assert(type->isFixedSize());
                    size32_t destSize = type->getMinSize();
                    builder.ensureCapacity(offset+destSize, field->name);
                    memcpy(builder.getSelf()+offset, source, copySize);
                    offset += copySize;
                    unsigned fillSize = destSize - copySize;
                    memset(builder.getSelf()+offset, match.fillChar, fillSize);
                    offset += fillSize;
                    break;
                }
                case match_typecast:
                    offset = translateScalar(builder, offset, field, type, sourceType, source);
                    break;
                case match_link:
                {
                    // a 32-bit record count, and a (linked) pointer to an array of record pointers
                    byte *dest = builder.ensureCapacity(offset+sizeof(size32_t)+sizeof(byte **), field->name)+offset;
                    *(size32_t *)dest = *(size32_t *)source;
                    *(byte ***)(dest + sizeof(size32_t)) = rtlLinkRowset(*(byte ***)(source + sizeof(size32_t)));
                    offset += sizeof(size32_t)+sizeof(byte **);
                    break;
                }
                case match_recurse:
                    if (type->getType()==type_record)
                        offset = match.subTrans->translate(builder, offset, source);
                    else if (type->isLinkCounted())
                    {
                        // a 32-bit record count, and a pointer to an array of record pointers
                        IEngineRowAllocator *childAllocator = builder.queryAllocator()->createChildRowAllocator(type->queryChildType());
                        assertex(childAllocator);  // May not be available when using serialized types (but unlikely to want to create linkcounted children remotely either)

                        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
                        builder.ensureCapacity(offset+sizeInBytes, field->name);
                        size32_t numRows = 0;
                        byte **childRows = nullptr;
                        if (sourceType->isLinkCounted())
                        {
                            // a 32-bit count, then a pointer to the source rows
                            size32_t childCount = *(size32_t *) source;
                            source += sizeof(size32_t);
                            const byte ** sourceRows = *(const byte***) source;
                            for (size32_t childRow = 0; childRow < childCount; childRow++)
                            {
                                RtlDynamicRowBuilder childBuilder(*childAllocator);
                                size32_t childLen = match.subTrans->translate(childBuilder, 0, sourceRows[childRow]);
                                childRows = childAllocator->appendRowOwn(childRows, ++numRows, (void *) childBuilder.finalizeRowClear(childLen));
                            }
                        }
                        else
                        {
                            // a 32-bit size, then rows inline
                            size32_t childSize = *(size32_t *) source;
                            source += sizeof(size32_t);
                            const byte *initialSource = source;
                            while ((size_t)(source - initialSource) < childSize)
                            {
                                RtlDynamicRowBuilder childBuilder(*childAllocator);
                                size32_t childLen = match.subTrans->translate(childBuilder, 0, source);
                                childRows = childAllocator->appendRowOwn(childRows, ++numRows, (void *) childBuilder.finalizeRowClear(childLen));
                                source += sourceType->queryChildType()->size(source, nullptr); // MORE - shame to repeat a calculation that the translate above almost certainly just did
                            }
                        }
                        // Go back in and patch the count, remembering it may have moved
                        rtlWriteInt4(builder.getSelf()+offset, numRows);
                        * ( const void * * ) (builder.getSelf()+offset+sizeof(size32_t)) = childRows;
                        offset += sizeInBytes;
                    }
                    else
                    {
                        size32_t countOffset = offset;
                        byte *dest = builder.ensureCapacity(offset+sizeof(size32_t), field->name)+offset;
                        offset += sizeof(size32_t);
                        size32_t initialOffset = offset;
                        *(size32_t *)dest = 0;  // patched below when true figure known
                        if (sourceType->isLinkCounted())
                        {
                            // a 32-bit count, then a pointer to the source rows
                            size32_t childCount = *(size32_t *) source;
                            source += sizeof(size32_t);
                            const byte ** sourceRows = *(const byte***) source;
                            for (size32_t childRow = 0; childRow < childCount; childRow++)
                            {
                                offset = match.subTrans->translate(builder, offset, sourceRows[childRow]);
                            }
                        }
                        else
                        {
                            // a 32-bit size, then rows inline
                            size32_t childSize = *(size32_t *) source;
                            source += sizeof(size32_t);
                            const byte *initialSource = source;
                            while ((size_t)(source - initialSource) < childSize)
                            {
                                offset = match.subTrans->translate(builder, offset, source);
                                source += sourceType->queryChildType()->size(source, nullptr); // MORE - shame to repeat a calculation that the translate above almost certainly just did
                            }
                        }
                        dest = builder.getSelf() + countOffset;  // Note - may have been moved by reallocs since last calculated
                        *(size32_t *)dest = offset - initialOffset;
                    }
                    break;
                default:
                    throwUnexpected();
                }
            }
        }
        if (estimate && offset-origOffset != estimate)
        {
            assert(offset-origOffset > estimate);  // Estimate is always supposed to be conservative
#ifdef TRACE_TRANSLATION
            DBGLOG("Wrote %u bytes to record (estimate was %u)\n", offset-origOffset, estimate);
#endif
        }
        return offset;
    }
    inline FieldMatchType match() const
    {
        return matchFlags;
    }
    bool canTranslate() const
    {
        return (matchFlags & match_fail) == 0;
    }
    bool needsTranslate() const
    {
        return (matchFlags & ~match_link) != 0;
    }
private:
    const RtlRecord &destRecInfo;
    const RtlRecord &sourceRecInfo;
    unsigned fixedDelta = 0;  // total size of all fixed-size source fields that are not matched
    UnsignedArray unmatched;  // List of all variable-size source fields that are unmatched
    FieldMatchType matchFlags = match_perfect;

    struct MatchInfo
    {
        unsigned matchIdx = 0;
        FieldMatchType matchType = match_fail;
        char fillChar = 0;
        GeneralRecordTranslator *subTrans = nullptr;
        ~MatchInfo()
        {
            delete subTrans;
        }
    } *matchInfo;

    static size32_t translateScalar(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, const RtlTypeInfo *destType, const RtlTypeInfo *sourceType, const byte *source)
    {
        // This code COULD move into rtlfield.cpp?
        switch(destType->getType())
        {
        case type_boolean:
        case type_int:
        case type_swapint:
        case type_packedint:
            offset = destType->buildInt(builder, offset, field, sourceType->getInt(source));
            break;
        case type_real:
            offset = destType->buildReal(builder, offset, field, sourceType->getReal(source));
            break;
        case type_decimal:  // Go via string - not common enough to special-case
        case type_data:
        case type_string:
        case type_varstring:
        case type_qstring:
        {
            size32_t size;
            rtlDataAttr text;
            sourceType->getString(size, text.refstr(), source);
            offset = destType->buildString(builder, offset, field, size, text.getstr());
            break;
        }
        case type_unicode:
        case type_varunicode:
        case type_utf8:
        {
            size32_t utf8chars;
            rtlDataAttr utf8Text;
            sourceType->getUtf8(utf8chars, utf8Text.refstr(), source);
            offset = destType->buildUtf8(builder, offset, field, utf8chars, utf8Text.getstr());
            break;
        }
        case type_set:
        {
            bool isAll = *(bool *) source;
            source+= sizeof(bool);
            byte *dest = builder.ensureCapacity(offset+sizeof(bool)+sizeof(size32_t), field->name)+offset;
            *(size32_t *) (dest + sizeof(bool)) = 0; // Patch later when size known
            offset += sizeof(bool) + sizeof(size32_t);
            if (isAll)
            {
                *(bool*) dest = true;
            }
            else
            {
                *(bool*) dest = false;
                size32_t sizeOffset = offset - sizeof(size32_t);  // Where we need to patch
                size32_t childSize = *(size32_t *)source;
                source += sizeof(size32_t);
                const byte *initialSource = source;
                size32_t initialOffset = offset;
                const RtlTypeInfo *destChildType = destType->queryChildType();
                const RtlTypeInfo *sourceChildType = sourceType->queryChildType();
                while ((size_t)(source - initialSource) < childSize)
                {
                    offset = translateScalar(builder, offset, field, destChildType, sourceChildType, source);
                    source += sourceChildType->size(source, nullptr); // MORE - shame to repeat a calculation that the translate above almost certainly just did
                }
                dest = builder.getSelf() + sizeOffset;  // Note - man have been moved by reallocs since last calculated
                *(size32_t *)dest = offset - initialOffset;
            }
            break;
        }
        default:
            throwUnexpected();
        }
        return offset;
    }

    size32_t estimateNewSize(const RtlRow &sourceRow) const
    {
        //DBGLOG("Source record size is %d", (int) sourceRow.getRecordSize());
        size32_t expectedSize = sourceRow.getRecordSize() - fixedDelta;
        //DBGLOG("Source record size without omitted fixed size fields is %d", expectedSize);
        ForEachItemIn(i, unmatched)
        {
            unsigned fieldNo = unmatched.item(i);
            expectedSize -= sourceRow.getSize(fieldNo);
            //DBGLOG("Reducing estimated size by %d to %d for omitted field %d (%s)", (int) sourceRow.getSize(fieldNo), expectedSize, fieldNo, sourceRecInfo.queryName(fieldNo));
        }
        if (matchFlags & ~(match_perfect|match_link|match_none|match_extend|match_truncate))
        {
            for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
            {
                const MatchInfo &match = matchInfo[idx];
                const RtlTypeInfo *type = destRecInfo.queryType(idx);
                unsigned matchField = match.matchIdx;
                switch (match.matchType)
                {
                case match_perfect:
                case match_link:
                case match_none:
                case match_extend:
                case match_truncate:
                    // These ones were already included in fixedDelta
                    break;
                default:
                    // This errs on the side of small - i.e. it assumes that all typecasts end up at minimum size
                    // We could do better in some cases e.g. variable string <-> variable unicode we can assume factor of 2,
                    // uft8 <-> string we could calculate here - but unlikely to be worth the effort.
                    // But it's fine for fixed size output fields, including truncate/extend
                    // We could also precalculate the expected delta if all omitted fields are fixed size - but not sure how likely/worthwhile that is.
                    expectedSize += type->getMinSize() - sourceRow.getSize(matchField);
                    //DBGLOG("Adjusting estimated size by (%d - %d) to %d for translated field %d (%s)", (int) sourceRow.getSize(matchField), type->getMinSize(), expectedSize, matchField, sourceRecInfo.queryName(matchField));
                    break;
                }
            }
        }
        return expectedSize;
    }
    void createMatchInfo()
    {
        for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
        {
            const RtlFieldInfo *field = destRecInfo.queryField(idx);
            const RtlTypeInfo *type = field->type;
            MatchInfo &info = matchInfo[idx];
            info.matchIdx = sourceRecInfo.getFieldNum(destRecInfo.queryName(idx));
            if (info.matchIdx == -1)
            {
                info.matchType = match_none;
                size32_t defaultSize = field->initializer ? type->size(field->initializer, nullptr) : type->getMinSize();
                fixedDelta -= defaultSize;
                //DBGLOG("Decreasing fixedDelta size by %d to %d for defaulted field %d (%s)", defaultSize, fixedDelta, idx, destRecInfo.queryName(idx));
            }
            else
            {
                const RtlTypeInfo *sourceType = sourceRecInfo.queryType(info.matchIdx);
                if (!type->isScalar() || !sourceType->isScalar())
                {
                    if (type->getType() != sourceType->getType())
                        info.matchType = match_fail;  // No translation from one non-scalar type to another
                    else
                    {
                        switch (type->getType())
                        {
                        case type_set:
                            if (type->queryChildType()->fieldType==sourceType->queryChildType()->fieldType &&
                                type->queryChildType()->length==sourceType->queryChildType()->length)
                                info.matchType = match_perfect;
                            else
                                info.matchType = match_typecast;
                            break;
                        case type_row:      // These are not expected I think...
                            throwUnexpected();
                        case type_record:
                        case type_table:
                        {
                            const RtlRecord *subDest = destRecInfo.queryNested(idx);
                            const RtlRecord *subSrc = sourceRecInfo.queryNested(info.matchIdx);
                            info.subTrans = new GeneralRecordTranslator(*subDest, *subSrc);
                            if (!info.subTrans->needsTranslate())
                            {
                                // Child does not require translation, but check linkcount mode matches too!
                                if (type->isLinkCounted())
                                    if (sourceType->isLinkCounted())
                                        info.matchType = match_link;
                                    else
                                        info.matchType = match_recurse;
                                else
                                    if (sourceType->isLinkCounted())
                                        info.matchType = match_recurse;
                                    else
                                        info.matchType = match_perfect;
                                if (info.matchType != match_recurse)
                                {
                                    delete info.subTrans;
                                    info.subTrans = nullptr;
                                }
                            }
                            else if (info.subTrans->canTranslate())
                            {
                                info.matchType = match_recurse;
                                matchFlags |= info.subTrans->matchFlags;
                            }
                            else
                                info.matchType = match_fail;
                            break;
                        }
                        default:
                            info.matchType = match_fail;
                            break;
                        }
                    }
                }
                else if (type->fieldType==sourceType->fieldType)
                {
                    if (type->length==sourceType->length)
                    {
                        info.matchType = match_perfect;
                    }
                    else
                    {
                        assert(type->isFixedSize());  // Both variable size would have matched length above
                        info.matchType = match_typecast;
                        if (type->length < sourceType->length)
                        {
                            if (type->canTruncate())
                            {
                                info.matchType = match_truncate;
                                fixedDelta += sourceType->getMinSize()-type->getMinSize();
                                //DBGLOG("Increasing fixedDelta size by %d to %d for truncated field %d (%s)", sourceType->getMinSize()-type->getMinSize(), fixedDelta, idx, destRecInfo.queryName(idx));
                            }
                        }
                        else
                        {
                            if (type->canExtend(info.fillChar))
                            {
                                info.matchType = match_extend;
                                fixedDelta += sourceType->getMinSize()-type->getMinSize();
                                //DBGLOG("Decreasing fixedDelta size by %d to %d for truncated field %d (%s)", type->getMinSize()-sourceType->getMinSize(), fixedDelta, idx, destRecInfo.queryName(idx));
                            }
                        }
                    }
                }
                else
                    info.matchType = match_typecast;
                // MORE - could note the highest interesting fieldnumber in the source and not bother filling in offsets after that
                // Not sure it would help much though - usually need to know the total record size anyway in real life
                if (idx != info.matchIdx)
                    matchFlags |= match_move;
            }
            matchFlags |= info.matchType;
        }
        if (sourceRecInfo.getNumFields() > destRecInfo.getNumFields())
            matchFlags |= match_remove;
        if (matchFlags && !destRecInfo.getFixedSize())
        {
            for (unsigned idx = 0; idx < sourceRecInfo.getNumFields(); idx++)
            {
                const RtlFieldInfo *field = sourceRecInfo.queryField(idx);
                const RtlTypeInfo *type = field->type;
                if (destRecInfo.getFieldNum(field->name) == (unsigned) -1)
                {
                    // unmatched field
                    if (type->isFixedSize())
                    {
                        //DBGLOG("Reducing estimated size by %d for (fixed size) omitted field %s", (int) type->getMinSize(), field->name);
                        fixedDelta += type->getMinSize();
                    }
                    else
                        unmatched.append(idx);
                }
            }
            //DBGLOG("Source record contains %d bytes of omitted fixed size fields", fixedDelta);
        }
    }
};

class TranslatedRowStream : public CInterfaceOf<IRowStream>
{
public:
    TranslatedRowStream(IRowStream *_inputStream, IEngineRowAllocator *_resultAllocator, const RtlRecord &outputRecord, const RtlRecord &inputRecord)
    : inputStream(_inputStream), resultAllocator(_resultAllocator), translator(outputRecord, inputRecord)
    {
        translator.describe();
    }
    virtual const void *nextRow()
    {
        if (eof)
            return NULL;
        const void *inRow = inputStream->nextRow();
        if (!inRow)
        {
            if (eogSeen)
                eof = true;
            else
                eogSeen = true;
            return nullptr;
        }
        else
            eogSeen = false;
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        size32_t len = translator.translate(rowBuilder, 0, (const byte *) inRow);
        rtlReleaseRow(inRow);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop() override
    {
        resultAllocator.clear();
    }
    bool canTranslate() const
    {
        return translator.canTranslate();
    }
    bool needsTranslate() const
    {
        return translator.needsTranslate();
    }
protected:
    Linked<IRowStream> inputStream;
    Linked<IEngineRowAllocator> resultAllocator;
    const GeneralRecordTranslator translator;
    unsigned numOffsets = 0;
    size_t * variableOffsets = nullptr;
    bool eof = false;
    bool eogSeen = false;
};

extern ECLRTL_API IRowStream * transformRecord(IEngineRowAllocator * resultAllocator,IOutputMetaData &  metaInput,IRowStream * input)
{
    if (resultAllocator->queryOutputMeta()==&metaInput)
        return LINK(input);
    Owned<TranslatedRowStream> stream = new TranslatedRowStream(input, resultAllocator,
                                                                resultAllocator->queryOutputMeta()->queryRecordAccessor(true),
                                                                metaInput.queryRecordAccessor(true));
    if (!stream->needsTranslate())
        return LINK(input);
    else if (!stream->canTranslate())
        rtlFail(0, "Cannot translate record stream");
    else
        return stream.getClear();
}

