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
#include "rtlnewkey.hpp"

//#define TRACE_TRANSLATION
#define VALIDATE_TYPEINFO_HASHES

#define RTLTYPEINFO_FORMAT_1   81   // In case we ever want to support more than one format or change how it is stored

//---------------------------------------------------------------------------------------------------------------------

extern ECLRTL_API RecordTranslationMode getTranslationMode(const char *val, bool isLocal)
{
    if (isEmptyString(val) || strToBool(val) || strieq(val, "payload"))
        return RecordTranslationMode::Payload;
    else if (strieq(val, "alwaysDisk") || strieq(val, "disk"))
    {
        if (!isLocal)
            WARNLOG("alwaysDisk translation mode should only ever be used via a HINT");
        return RecordTranslationMode::AlwaysDisk;
    }
    else if (strieq(val, "alwaysECL") || strieq(val, "ecl"))
    {
        if (isLocal)
            return RecordTranslationMode::AlwaysECL;
        WARNLOG("Unsupported alwaysECL translation mode used globally, translation disabled - use with HINT to set locally.");
    }
    return RecordTranslationMode::None;
}

extern ECLRTL_API const char *getTranslationModeText(RecordTranslationMode val)
{
    switch (val)
    {
    case RecordTranslationMode::AlwaysDisk: return "alwaysDisk";
    case RecordTranslationMode::AlwaysECL: return "alwaysECL";
    case RecordTranslationMode::Payload: return "payload";
    case RecordTranslationMode::None: return "off";
    }
    throwUnexpected();
}


//---------------------------------------------------------------------------------------------------------------------

const RtlTypeInfo *FieldTypeInfoStruct::createRtlTypeInfo() const
{
    const RtlTypeInfo *ret = nullptr;
    switch (fieldType & RFTMkind)
    {
    case type_boolean:
        ret = new RtlBoolTypeInfo(fieldType, length);
        break;
    case type_keyedint:
        ret = new RtlKeyedIntTypeInfo(fieldType, length, childType);
        break;
    case type_int:
        ret = new RtlIntTypeInfo(fieldType, length);
        break;
    case type_blob:
        ret = new RtlBlobTypeInfo(fieldType, length, childType);
        break;
    case type_filepos:
#if __BYTE_ORDER == __LITTLE_ENDIAN
        ret = new RtlSwapIntTypeInfo(fieldType, length);
#else
        ret = new RtlIntTypeInfo(fieldType, length);
#endif
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
    case type_dictionary:
        assert(childType);
        ret = new RtlDictionaryTypeInfo(fieldType, length, childType);
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
    case type_ifblock:
        ret = new RtlDynamicIfBlockTypeInfo(fieldType, length, fieldsArray, nullptr, filter);
        break;
    case type_alien:
        assert(childType);
        ret = new RtlAlienTypeInfo(fieldType, length, childType);
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
            const RtlTypeInfo *childType = type->queryChildType();
            if (childType)
                serializeType(childType);
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
            throw makeStringException(MSGAUD_user, 1000, "This type structure cannot be serialized");
        addPropHex("fieldType", type->fieldType);
        addProp("length", type->length);
        addPropNonEmpty("locale", type->queryLocale());
        const RtlTypeInfo *childType = type->queryChildType();
        if (childType)
            addPropType("child", childType);
        const IFieldFilter * filter = type->queryFilter();
        if (filter)
        {
            StringBuffer filterText;
            filter->serialize(filterText);
            addPropType("filterType", &filter->queryType());
            addProp("filter", filterText);
        }
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
                    if (isVirtualInitializer(child->initializer))
                        addProp("vinit", getVirtualInitializer(child->initializer));
                    else
                        addProp("init", child->type->size((const byte *) child->initializer, nullptr), (const byte *) child->initializer);
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
         int oldEnd = out.setEndian(__LITTLE_ENDIAN);
         CRtlFieldTypeBinSerializer s(out);
         byte format = RTLTYPEINFO_FORMAT_1;
         out.append(format);
         DelayedMarker<hash64_t> hash(out);
         DelayedSizeMarker size(out);
         size32_t pos = out.length();
         s.serializeType(type);
         size.write();
         hash.write(rtlHash64Data(size.size(), out.toByteArray()+pos, 0));
         out.setEndian(oldEnd);
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
                for (unsigned idx = 0;;idx++)
                {
                    const RtlFieldInfo * child = fields[idx];
                    if (!child)
                        break;
                    serializeType(child->type);
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
            throw makeStringException(MSGAUD_user, 1000, "This type structure cannot be serialized");
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
        out.appendPacked(type->length);
        if (fieldType & RFTMhasLocale)
            out.append(locale);
        if (child)
            out.appendPacked(queryTypeIdx(child));
        const IFieldFilter * filter = type->queryFilter();
        if (filter)
        {
            out.appendPacked(queryTypeIdx(&filter->queryType()));
            filter->serialize(out);
        }
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
                {
                    if (isVirtualInitializer(child->initializer))
                        flags |= RFTMhasVirtualInitializer;
                    else
                        flags |= RFTMhasInitializer;
                }
                out.append(flags);
                if (child->xpath)
                    out.append(child->xpath);
                // initializer is tricky - it's not (in general) a null-terminated string but the actual length is not easily available
                if (flags & RFTMhasInitializer)
                {
                    unsigned initLength = child->type->size((const byte *) child->initializer, nullptr);
                    out.appendPacked(initLength).append(initLength, (const byte *) child->initializer);
                }
                else if (flags &RFTMhasVirtualInitializer)
                    out.append(getVirtualInitializer(child->initializer));
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
     * @param  _callback Supplies a callback to be used for blobs/filepositions.
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
        // First allow the types to clean up any critical cached information, then delete them in a second pass
        HashIterator allTypes(types);
        ForEach(allTypes)
        {
            const RtlTypeInfo **type = types.mapToValue(&allTypes.query());
            cleanupType(*type);
        }
        cleanupType(base);

        ForEach(allTypes)
        {
            const RtlTypeInfo **type = types.mapToValue(&allTypes.query());
            deleteType(*type);
        }
        deleteType(base);
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
     * @param  _jsonTree JSON property tree to be deserialized, as created by CRtlFieldTypeSerializer
     * @return Deserialized type object
     */
    virtual const RtlTypeInfo *deserialize(IPropertyTree &jsonTree) override
    {
        assertex(!base);
        base = deserializeType(&jsonTree, &jsonTree);
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
     * @param  buf Binary serialized typeinfo to be deserialized, as created by CRtlFieldTypeSerializer
     * @return Deserialized type object
     */
    virtual const RtlTypeInfo *deserialize(MemoryBuffer &buf) override
    {
        assertex(!base);
        unsigned nextTypeNum = 0;
        int oldEndian = buf.setEndian(__LITTLE_ENDIAN);
        try
        {
            byte format;
            buf.read(format);
            if (format != RTLTYPEINFO_FORMAT_1)
                throw MakeStringException(0, "Invalid type info (%d) in CRtlFieldTypeDeserializer::deserialize", format);
            hash64_t hash;
            buf.read(hash);
            size32_t size;
            buf.read(size);
#ifdef VALIDATE_TYPEINFO_HASHES
            hash64_t expected = rtlHash64Data(size, buf.readDirect(0), 0);
            if (expected != hash)
                throw MakeStringException(0, "Invalid type info hash in CRtlFieldTypeDeserializer::deserialize");
#endif
            size32_t endpos = buf.getPos() + size;
            while (buf.getPos() < endpos)
            {
                if (base)
                {
                    addType(base, nextTypeNum++);
                    base = nullptr;  // in case of exceptions...
                }
                base = deserializeType(buf);
            }
            if (buf.getPos()!=endpos)
                throw MakeStringException(0, "Invalid type info (incorrect size data) in CRtlFieldTypeDeserializer::deserialize");
            buf.setEndian(oldEndian);
            return base;
        }
        catch(...)
        {
            buf.setEndian(oldEndian);
            throw;
        }
    }

    virtual const RtlTypeInfo *addType(FieldTypeInfoStruct &info, const IInterface *typeOrIfblock) override
    {
        VStringBuffer name("%p", typeOrIfblock);
        const RtlTypeInfo ** found = types.getValue(name);
        if (found)
            return *found;
        savedTypes.append(LINK(typeOrIfblock));
        info.locale = keep(info.locale);
        const RtlTypeInfo * ret = info.createRtlTypeInfo();
        types.setValue(name, ret);
        unsigned baseType = (info.fieldType & RFTMkind);
        if (baseType == type_record)
            patchIfBlockParentRow(ret, static_cast<const RtlRecordTypeInfo *>(ret));
        return ret;
    }

    virtual const RtlTypeInfo *lookupType(const IInterface * typeOrIfBlock) const override
    {
        VStringBuffer name("%p", typeOrIfBlock);
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
    IConstPointerArray savedTypes; // ensure types remain alive for subsequent lookups
    void deleteType(const RtlTypeInfo *type)
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
                    if (!isVirtualInitializer(child->initializer))
                        free((void *)child->initializer);
                    delete child;
                    cur++;
                }
                delete [] fields;
            }
            type->doDelete();
        }
    }
    void cleanupType(const RtlTypeInfo *type)
    {
        if (type)
            type->doCleanup();
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
        unsigned baseType = (info.fieldType & RFTMkind);
        if ((baseType == type_record) || (baseType == type_ifblock))
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
                const char *fieldVInit = field.queryProp("vinit");
                if (fieldInit)
                {
                    StringBuffer decoded;
                    JBASE64_Decode(fieldInit, decoded);
                    fieldInit = decoded.detach(); // NOTE - this gets freed in cleanupType()
                }
                else if (fieldVInit)
                {
                    fieldInit = (const char  *)(memsize_t)atoi(fieldVInit);
                }
                info.fieldsArray[n] = new RtlFieldStrInfo(fieldName, fieldXpath, lookupType(fieldTypeName, all), flags, fieldInit);
                n++;
            }
        }
        if (baseType == type_ifblock)
        {
            //Filter field needs to be deserialized and the type resolved separately outside the deserialize call
            //because there isn't a RtlTypeInfo available to resolve the field (since we are currently deserializing it!)
            const char * filterText = type->queryProp("filter");
            StringBuffer fieldIdText;
            readFieldFromFieldFilter(fieldIdText, filterText);
            unsigned fieldId = atoi(fieldIdText);
            const RtlTypeInfo * fieldType = lookupType(type->queryProp("filterType"), all);
            info.filter = deserializeFieldFilter(fieldId, *fieldType, filterText);
        }

        const RtlTypeInfo * result = info.createRtlTypeInfo();
        if (baseType == type_record)
            patchIfBlockParentRow(result, static_cast<const RtlRecordTypeInfo *>(result));
        return result;
    }

    const RtlTypeInfo *deserializeType(MemoryBuffer &type)
    {
        FieldTypeInfoStruct info;
        type.read(info.fieldType);
        type.readPacked(info.length);
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

        unsigned baseType = (info.fieldType & RFTMkind);
        if (baseType == type_ifblock)
        {
            unsigned childIdx;
            type.readPacked(childIdx);
            const RtlTypeInfo * fieldType = lookupType(childIdx);
            unsigned fieldId;
            type.readPacked(fieldId);
            info.filter = deserializeFieldFilter(fieldId, *fieldType, type);
        }

        if (info.fieldType & RFTMhasFields)
        {
            unsigned numFields;
            type.readPacked(numFields);
            info.fieldsArray = new const RtlFieldInfo * [numFields+1];
            info.fieldsArray[numFields] = nullptr;
            for (unsigned n = 0; n < numFields; n++)
            {
                const char *fieldName;
                type.read(fieldName);
                if (fieldName[0] == '\0')
                    fieldName = nullptr;
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
                else if (fieldFlags & RFTMhasVirtualInitializer)
                {
                    byte virtualKind;
                    type.read(virtualKind);
                    init = (void *)(memsize_t)virtualKind;
                }
                fieldFlags &= ~RFTMserializerFlags;
                info.fieldsArray[n] = new RtlFieldStrInfo(keep(fieldName), keep(xpath), lookupType(fieldType), fieldFlags, (const char *) init);
            }
        }
        info.fieldType &= ~RFTMserializerFlags;
        const RtlTypeInfo * result = info.createRtlTypeInfo();
        if (baseType == type_record)
            patchIfBlockParentRow(result, static_cast<const RtlRecordTypeInfo *>(result));
        return result;
    }

    void patchIfBlockParentRow(const RtlTypeInfo * fieldType, const RtlRecordTypeInfo * parentRow)
    {
        const RtlFieldInfo * const * fields = fieldType->queryFields();
        for (;*fields;fields++)
        {
            const RtlFieldInfo * cur = *fields;
            if (!cur)
                break;

            const RtlTypeInfo * curType = cur->type;
            if ((curType->fieldType & RFTMkind) == type_ifblock)
            {
                const RtlDynamicIfBlockTypeInfo * constifblock = static_cast<const RtlDynamicIfBlockTypeInfo *>(curType);
                RtlDynamicIfBlockTypeInfo * ifblock = const_cast<RtlDynamicIfBlockTypeInfo *>(constifblock);
                ifblock->setParent(parentRow);
                patchIfBlockParentRow(curType, parentRow);
            }
        }
    }
};

extern ECLRTL_API IRtlFieldTypeDeserializer *createRtlFieldTypeDeserializer()
{
    return new CRtlFieldTypeDeserializer();
}

extern ECLRTL_API StringBuffer &dumpTypeInfo(StringBuffer &ret, const RtlTypeInfo *t)
{
    return CRtlFieldTypeSerializer::serialize(ret, t);
}

extern ECLRTL_API bool dumpTypeInfo(MemoryBuffer &ret, const RtlTypeInfo *t)
{
    try
    {
        CRtlFieldTypeBinSerializer::serialize(ret, t);
        return true;
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
        return false;
    }
}

extern ECLRTL_API void serializeRecordType(size32_t & __lenResult, void * & __result, IOutputMetaData &  metaVal)
{
    MemoryBuffer ret;
    try
    {
        CRtlFieldTypeBinSerializer::serialize(ret, metaVal.queryTypeInfo());
    }
    catch (IException * e)
    {
        ret.clear();
        e->Release();
    }
    __lenResult = ret.length();
    __result = ret.detach();
}

extern ECLRTL_API void dumpRecordType(size32_t & __lenResult,char * & __result,IOutputMetaData &metaVal)
{
    StringBuffer ret;
    try
    {
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
    }
    catch (IException * e)
    {
        e->errorMessage(ret.clear());
        e->Release();
    }

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
        if ((unsigned) column < r.getNumFields())
        {
            unsigned numOffsets = r.getNumVarFields() + 1;
            size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
            RtlRow offsetCalculator(r, row, numOffsets, variableOffsets);
            offsetCalculator.getUtf8(__lenResult, __result, column);
        }
    }
}

extern ECLRTL_API int getFieldNum(const char *fieldName, IOutputMetaData &  metaVal)
{
    const RtlRecord r = metaVal.queryRecordAccessor(true);
    return r.getFieldNum(fieldName);
}
enum FieldMatchType {
    // On a field, exactly one of the below is set, but translator returns a bitmap indicating
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
    match_keychange   = 0x200,   // at least one affected field not marked as payload (set on translator)
    match_virtual     = 0x800,   // at least one affected field is a virtual field (set on translator)

    // This flag may be set in conjunction with the others
    match_inifblock   = 0x400,   // matching to a field in an ifblock - may not be present
    match_deblob      = 0x1000,  // source needs fetching from a blob prior to translation
    match_dynamic     = 0x2000,  // source needs fetching from dynamic source (callback)
    match_filepos     = 0x4000,  // type moving in or out of filepos field - cast required
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
    if (flags & match_inifblock) out.append("|ifblock");
    if (flags & match_keychange) out.append("|keychange");
    if (flags & match_fail) out.append("|fail");
    if (flags & match_virtual) out.append("|virtual");
    if (flags & match_deblob) out.append("|blob");
    if (flags & match_dynamic) out.append("|dynamic");
    if (flags & match_filepos) out.append("|filepos");
    assertex(out.length() > origlen);
    return out.remove(origlen, 1);
}

inline constexpr FieldMatchType operator|(FieldMatchType a, FieldMatchType b) { return (FieldMatchType)((int)a | (int)b); }
inline FieldMatchType &operator|=(FieldMatchType &a, FieldMatchType b) { return (FieldMatchType &) ((int &)a |= (int)b); }

class GeneralRecordTranslator : public CInterfaceOf<IDynamicTransform>
{
public:
    GeneralRecordTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo, bool _binarySource, type_vals _callbackRawType = type_any)
        : destRecInfo(_destRecInfo), sourceRecInfo(_srcRecInfo), binarySource(_binarySource), callbackRawType(_callbackRawType)
    {
        matchInfo = new MatchInfo[destRecInfo.getNumFields()];
        createMatchInfo();
#ifdef _DEBUG
        //describe();
#endif
    }
    ~GeneralRecordTranslator()
    {
        delete [] matchInfo;
    }
// IDynamicTransform impl.
    virtual void describe() const override
    {
        doDescribe(0);
    }
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const byte *sourceRec) const override
    {
        assertex(binarySource);
        return doTranslate(builder, callback, 0, sourceRec);
    }
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const RtlRow &sourceRow) const override
    {
        assertex(binarySource);
        sourceRow.lazyCalcOffsets(-1);  // MORE - could save the max one we actually need...
        return doTranslateOpaqueType(builder, callback, 0, &sourceRow);
    }
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const IDynamicFieldValueFetcher & fetcher) const override
    {
        assertex(!binarySource);
        return doTranslateOpaqueType(builder, callback, 0, &fetcher);
    }
    virtual bool canTranslate() const override
    {
        return (matchFlags & match_fail) == 0;
    }
    virtual bool needsTranslate() const override
    {
        return !binarySource || (matchFlags & ~(match_link|match_inifblock)) != 0;
    }
    virtual bool needsNonVirtualTranslate() const override
    {
        return (matchFlags & ~(match_link|match_virtual|match_keychange|match_inifblock)) != 0;
    }
    virtual bool keyedTranslated() const override
    {
        return (matchFlags & match_keychange) != 0;
    }
private:
    void doDescribe(unsigned indent) const
    {
        unsigned perfect=0;
        unsigned reported=0;
        for (unsigned idx = 0; idx <  destRecInfo.getNumFields(); idx++)
        {
            const char *source = destRecInfo.queryName(idx);
            const MatchInfo &match = matchInfo[idx];
            if (match.matchType == match_none)
                DBGLOG("%*sNo match for field %s - default value will be used", indent, "", source);
            else if (match.matchType == match_virtual)
                DBGLOG("%*sUse virtual value for field %s", indent, "", source);
            else
            {
                if (match.matchType != match_perfect)
                {
                    reported++;
                    StringBuffer matchStr;
                    DBGLOG("%*sMatch (%s) to field %d for field %s (typecode %x)", indent, "", describeFlags(matchStr, match.matchType).str(), match.matchIdx, source, destRecInfo.queryType(idx)->fieldType);
                    if (match.subTrans)
                        match.subTrans->doDescribe(indent+2);
                }
                else
                    perfect++;
            }
        }
        if (allUnmatched.ordinality())
        {
            VStringBuffer msg("%*sDropped field", indent, "");
            if (allUnmatched.ordinality()>1)
                msg.append('s');
            for (unsigned idx = 0; idx < allUnmatched.ordinality() && idx < 5; idx++)
            {

                if (idx)
                    msg.append(',');
                msg.appendf(" %s", sourceRecInfo.queryName(allUnmatched.item(idx)));
            }
            if (allUnmatched.ordinality() > 5)
                msg.appendf(" and %u other fields", allUnmatched.ordinality() - 5);
            DBGLOG("%s", msg.str());
        }
        if (!canTranslate())
            DBGLOG("%*sTranslation is NOT possible", indent, "");
        else if (needsTranslate())
        {
            StringBuffer matchStr;
            if (perfect)
                DBGLOG("%u %sfield%s matched perfectly", perfect, reported ? "other " : "", perfect==1 ? "" : "s");
            DBGLOG("%*sTranslation is possible (%s)", indent, "", describeFlags(matchStr, matchFlags).str());
        }
        else
            DBGLOG("%*sTranslation is not necessary", indent, "");
    }
    size32_t doTranslate(ARowBuilder &builder, IVirtualFieldCallback & callback, size32_t offset, const byte *sourceRec) const
    {
        unsigned numOffsets = sourceRecInfo.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow sourceRow(sourceRecInfo, sourceRec, numOffsets, variableOffsets);  // MORE - could save the max source offset we actually need, and only set up that many...
        return doTranslateOpaqueType(builder, callback, offset, &sourceRow);
    }
    size32_t doTranslateOpaqueType(ARowBuilder &builder, IVirtualFieldCallback & callback, size32_t offset, const void *sourceRow) const
    {
        dbgassertex(canTranslate());
        byte * destConditions = (byte *)alloca(destRecInfo.getNumIfBlocks() * sizeof(byte));
        memset(destConditions, 2, destRecInfo.getNumIfBlocks() * sizeof(byte));
        size32_t estimate = destRecInfo.getFixedSize();
        bool hasBlobs = false;
        if (!estimate)
        {
            if (binarySource)
                estimate = estimateNewSize(*(const RtlRow *)sourceRow);
            else
                estimate = destRecInfo.getMinRecordSize();
            builder.ensureCapacity(offset+estimate, "record");
        }
        size32_t origOffset = offset;
        for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
        {
            const RtlFieldInfo *field = destRecInfo.queryField(idx);
            if (field->omitable() && destRecInfo.excluded(field, builder.getSelf(), destConditions))
                continue;
            const RtlTypeInfo *type = field->type;
            const MatchInfo &match = matchInfo[idx];
            if (match.matchType == match_none || match.matchType==match_fail)
            {
                offset = type->buildNull(builder, offset, field);
            }
            else if (match.matchType == match_virtual)
            {
                switch (getVirtualInitializer(field->initializer))
                {
                case FVirtualFilePosition:
                    offset = type->buildInt(builder, offset, field, callback.getFilePosition(sourceRow));
                    break;
                case FVirtualLocalFilePosition:
                    offset = type->buildInt(builder, offset, field, callback.getLocalFilePosition(sourceRow));
                    break;
                case FVirtualFilename:
                    {
                        const char * filename = callback.queryLogicalFilename(sourceRow);
                        offset = type->buildString(builder, offset, field, strlen(filename), filename);
                        break;
                    }
                default:
                    throwUnexpected();
                }
            }
            else
            {
                unsigned matchField = match.matchIdx;
                const RtlTypeInfo *sourceType = sourceRecInfo.queryType(matchField);

                size_t sourceOffset = 0;
                const byte *source = nullptr;
                size_t copySize = 0;
                if (binarySource)
                {
                    const RtlRow &rtlRow = *(const RtlRow *)sourceRow;
                    sourceOffset = rtlRow.getOffset(matchField);
                    source = rtlRow.queryRow() + sourceOffset;
                    copySize = rtlRow.getSize(matchField);
                }
                if (match.matchType & match_deblob)
                {
                    offset_t blobId = sourceType->getInt(source);
                    sourceType = sourceType->queryChildType();
                    sourceOffset = 0;
                    source = callback.lookupBlob(blobId);
                    copySize = sourceType->size(source, source);
                    hasBlobs = true;
                }
                if (copySize == 0 && (match.matchType & match_inifblock))  // Field is missing because of an ifblock - use default value
                {
                    offset = type->buildNull(builder, offset, field);
                }
                else
                {
                    switch (match.matchType & ~(match_inifblock|match_deblob))
                    {
                    case match_perfect:
                    {
                        // Look ahead for other perfect matches and combine the copies
                        if (!(match.matchType & match_deblob))
                        {
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
                            copySize = ((const RtlRow *)sourceRow)->getOffset(matchField+1) - sourceOffset;
                        }
                        builder.ensureCapacity(offset+copySize, field->name);
                        memcpy(builder.getSelf()+offset, source, copySize);
                        offset += copySize;
                        break;
                    }
                    case match_truncate:
                    {
                        assert(type->isFixedSize());
                        copySize = type->getMinSize();
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
                    case match_filepos:
                    case match_typecast:
                        offset = translateScalar(builder, offset, field, *type, *sourceType, source);
                        break;
                    case match_typecast|match_dynamic:
                    {
                        const IDynamicFieldValueFetcher &callbackRowHandler = *(const IDynamicFieldValueFetcher *)sourceRow;
                        source = callbackRowHandler.queryValue(matchField, copySize);
                        if (callbackRawType == type_string)
                            offset = translateScalarFromString(builder, offset, field, *type, *sourceType, (const char *)source, (size_t)copySize);
                        else
                            offset = translateScalarFromUtf8(builder, offset, field, *type, *sourceType, (const char *)source, (size_t)copySize);
                        break;
                    }
                    case match_link:
                    {
                        // a 32-bit record count, and a (linked) pointer to an array of record pointers
                        byte *dest = builder.ensureCapacity(offset+sizeof(size32_t)+sizeof(const byte **), field->name)+offset;
                        *(size32_t *)dest = *(size32_t *)source;
                        *(const byte ***)(dest + sizeof(size32_t)) = rtlLinkRowset(*(const byte ***)(source + sizeof(size32_t)));
                        offset += sizeof(size32_t)+sizeof(const byte **);
                        break;
                    }
                    case match_recurse|match_dynamic:
                    {
                        const IDynamicFieldValueFetcher &callbackRowHandler = *(const IDynamicFieldValueFetcher *)sourceRow;
                        Owned<IDynamicRowIterator> iterator = callbackRowHandler.getNestedIterator(matchField);
                        if (type->getType()==type_record)
                        {
                            IDynamicFieldValueFetcher &fieldFetcher = iterator->query();
                            offset = match.subTrans->doTranslateOpaqueType(builder, callback, offset, &fieldFetcher);
                        }
                        else if (type->isLinkCounted())
                        {
                            // a 32-bit record count, and a pointer to an array of record pointers
                            IEngineRowAllocator *childAllocator = builder.queryAllocator()->createChildRowAllocator(type->queryChildType());
                            assertex(childAllocator);  // May not be available when using serialized types (but unlikely to want to create linkcounted children remotely either)

                            size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
                            builder.ensureCapacity(offset+sizeInBytes, field->name);
                            size32_t numRows = 0;
                            const byte **childRows = nullptr;
                            ForEach(*iterator)
                            {
                                IDynamicFieldValueFetcher &fieldFetcher = iterator->query();
                                RtlDynamicRowBuilder childBuilder(*childAllocator);
                                size32_t childLen = match.subTrans->doTranslateOpaqueType(childBuilder, callback, 0, &fieldFetcher);
                                childRows = childAllocator->appendRowOwn(childRows, ++numRows, (void *) childBuilder.finalizeRowClear(childLen));
                            }
                            if (type->getType() == type_dictionary)
                            {
                                const RtlTypeInfo * childType = type->queryChildType();
                                assertex(childType && childType->getType() == type_record);
                                CHThorHashLookupInfo lookupHelper(static_cast<const RtlRecordTypeInfo &>(*childType));
                                rtlCreateDictionaryFromDataset(numRows, childRows, childAllocator, lookupHelper);
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
                            ForEach(*iterator)
                            {
                                IDynamicFieldValueFetcher &fieldFetcher = iterator->query();
                                offset = match.subTrans->doTranslateOpaqueType(builder, callback, offset, &fieldFetcher);
                            }
                            dest = builder.getSelf() + countOffset;  // Note - may have been moved by reallocs since last calculated
                            *(size32_t *)dest = offset - initialOffset;
                        }
                        break;
                    }
                    case match_recurse:
                        if (type->getType()==type_record)
                            offset = match.subTrans->doTranslate(builder, callback, offset, source);
                        else if (type->isLinkCounted())
                        {
                            // a 32-bit record count, and a pointer to an array of record pointers
                            Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(type->queryChildType());
                            assertex(childAllocator);  // May not be available when using serialized types (but unlikely to want to create linkcounted children remotely either)

                            size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
                            builder.ensureCapacity(offset+sizeInBytes, field->name);
                            size32_t numRows = 0;
                            const byte **childRows = nullptr;
                            if (sourceType->isLinkCounted())
                            {
                                // a 32-bit count, then a pointer to the source rows
                                size32_t childCount = *(size32_t *) source;
                                source += sizeof(size32_t);
                                const byte ** sourceRows = *(const byte***) source;
                                for (size32_t childRow = 0; childRow < childCount; childRow++)
                                {
                                    RtlDynamicRowBuilder childBuilder(*childAllocator);
                                    size32_t childLen = match.subTrans->doTranslate(childBuilder, callback, 0, sourceRows[childRow]);
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
                                    size32_t childLen = match.subTrans->doTranslate(childBuilder, callback, 0, source);
                                    childRows = childAllocator->appendRowOwn(childRows, ++numRows, (void *) childBuilder.finalizeRowClear(childLen));
                                    source += sourceType->queryChildType()->size(source, nullptr); // MORE - shame to repeat a calculation that the translate above almost certainly just did
                                }
                            }
                            if (type->getType() == type_dictionary)
                            {
                                const RtlTypeInfo * childType = type->queryChildType();
                                assertex(childType && childType->getType() == type_record);
                                CHThorHashLookupInfo lookupHelper(static_cast<const RtlRecordTypeInfo &>(*childType));
                                rtlCreateDictionaryFromDataset(numRows, childRows, childAllocator, lookupHelper);
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
                                    const byte * row = sourceRows[childRow];
                                    //Dictionaries have blank rows - ignore them when serializing (to a dataset)
                                    if (row)
                                        offset = match.subTrans->doTranslate(builder, callback, offset, row);
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
                                    offset = match.subTrans->doTranslate(builder, callback, offset, source);
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
        }
        if (estimate && offset-origOffset != estimate)
        {
            if (offset == origOffset)
            {
                //Zero size records are treated as single byte to avoid confusion with sizes returned from transforms etc.
                offset++;
            }
            else
            {
                if (!hasBlobs)
                    assert(offset-origOffset > estimate);  // Estimate is always supposed to be conservative
    #ifdef TRACE_TRANSLATION
                DBGLOG("Wrote %u bytes to record (estimate was %u)\n", offset-origOffset, estimate);
    #endif
            }
        }
        return offset;
    }
    inline FieldMatchType match() const
    {
        return matchFlags;
    }
    const RtlRecord &destRecInfo;
    const RtlRecord &sourceRecInfo;
    bool binarySource = true;
    type_vals callbackRawType;
    int fixedDelta = 0;  // total size difference from all fixed size mappings
    UnsignedArray allUnmatched;  // List of all source fields that are unmatched (so that we can trace them)
    UnsignedArray variableUnmatched;  // List of all variable-size source fields that are unmatched
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

    static size32_t translateScalarFromUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, const RtlTypeInfo &destType, const RtlTypeInfo &sourceType, const char *source, size_t srcSize)
    {
        switch(destType.getType())
        {
        case type_boolean:
        case type_int:
        case type_swapint:
        case type_packedint:
        case type_filepos:
        case type_keyedint:
        {
            __int64 res = rtlStrToInt8(srcSize, source);
            offset = destType.buildInt(builder, offset, field, res);
            break;
        }
        case type_real:
        {
            double res = rtlStrToReal(srcSize, source);
            offset = destType.buildReal(builder, offset, field, res);
            break;
        }
        case type_data:
        case type_string:
        case type_decimal:  // Go via string - not common enough to special-case
        case type_varstring:
        case type_qstring:
        case type_utf8:
            //MORE: Could special case casting from utf8 to utf8 similar to strings above
        case type_unicode:
        case type_varunicode:
        {
            size32_t utf8chars = rtlUtf8Length(srcSize, source);
            offset = destType.buildUtf8(builder, offset, field, utf8chars, source);
            break;
        }
        case type_set:
        {
            UNIMPLEMENTED; // JCS->GH - but perhaps can/should translate using iterator too?
            break;
        }
        default:
            throwUnexpected();
        }
        return offset;
    }
    static size32_t translateScalarFromString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, const RtlTypeInfo &destType, const RtlTypeInfo &sourceType, const char *source, size_t srcSize)
    {
        switch(destType.getType())
        {
        case type_boolean:
        case type_int:
        case type_swapint:
        case type_packedint:
        case type_filepos:
        case type_keyedint:
        {
            __int64 res = rtlStrToInt8(srcSize, source);
            offset = destType.buildInt(builder, offset, field, res);
            break;
        }
        case type_real:
        {
            double res = rtlStrToReal(srcSize, source);
            offset = destType.buildReal(builder, offset, field, res);
            break;
        }
        case type_data:
        case type_string:
        case type_decimal:  // Go via string - not common enough to special-case
        case type_varstring:
        case type_qstring:
        case type_utf8:
            //MORE: Could special case casting from utf8 to utf8 similar to strings above
        case type_unicode:
        case type_varunicode:
        {
            offset = destType.buildString(builder, offset, field, srcSize, source);
            break;
        }
        case type_set:
        {
            UNIMPLEMENTED; // JCS->GH - but perhaps can/should translate using iterator too?
            break;
        }
        default:
            throwUnexpected();
        }
        return offset;
    }
    static bool canTranslateNonScalar(const RtlTypeInfo * type, const RtlTypeInfo * sourceType)
    {
        auto target = type->getType();
        auto source = sourceType->getType();
        if (target == source)
            return true;
        if ((target == type_dictionary) && (source == type_table))
            return true;
        if ((target == type_table) && (source == type_dictionary))
            return true;
        return false;
    }
    void createMatchInfo()
    {
        unsigned defaulted = 0;
        bool destHasNested = destRecInfo.hasNested();
        bool sourceHasNested = sourceRecInfo.hasNested();
        for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
        {
            const RtlFieldInfo *field = destRecInfo.queryField(idx);
            const RtlTypeInfo *type = field->type;
            MatchInfo &info = matchInfo[idx];
            const char *name = destRecInfo.queryName(idx);
            info.matchIdx = sourceRecInfo.getFieldNum(name);
            if (info.matchIdx == (unsigned) -1)
            {
                const byte * initializer = (const byte *) field->initializer;
                info.matchType = isVirtualInitializer(initializer) ? match_virtual : match_none;
                if ((field->flags & RFTMinifblock) == 0)
                {
                    size32_t defaultSize = (initializer && !isVirtualInitializer(initializer)) ? type->size(initializer, nullptr) : type->getMinSize();
                    fixedDelta -= defaultSize;
#ifdef TRACE_TRANSLATION
                    DBGLOG("Decreasing fixedDelta size by %d to %d for defaulted field %d (%s)", defaultSize, fixedDelta, idx, destRecInfo.queryName(idx));
#endif
                }
                if ((field->flags & RFTMispayloadfield) == 0)
                    matchFlags |= match_keychange;
                defaulted++;
                // If dest field is in a nested record, we need to check that there's no "non-record" field in source matching current nested record name
                if (name)
                {
                    if (destHasNested)
                    {
                        const char *ldot = strrchr(name, '.');
                        if (ldot)
                        {
                            StringBuffer recname(ldot-name, name);
                            if (sourceRecInfo.getFieldNum(recname) != (unsigned) -1)
                                info.matchType = match_fail;  // No translation from non-record to record
                        }
                    }
                    if (sourceHasNested && sourceRecInfo.queryOriginalField(name))
                    {
                        // Similarly if dest field IS not a nested record, but there is a field in source which is.
                        // Note that we already know there is no matching field called name in the exapanded version of source,
                        // so any match we find must be a record
                        info.matchType = match_fail;  // No translation from record to non-record
                    }
                }
            }
            else
            {
                bool deblob = false;
                const RtlTypeInfo *sourceType = sourceRecInfo.queryType(info.matchIdx);
                unsigned sourceFlags = sourceRecInfo.queryField(info.matchIdx)->flags;
                unsigned destFlags = field->flags;
                if (binarySource && sourceType->isBlob())
                {
                    if (type->isBlob())
                    {
                    }
                    else
                    {
                        sourceType = sourceType->queryChildType();
                        deblob = true;
                    }
                }
                if (!type->isScalar() || !sourceType->isScalar())
                {
                    if (!canTranslateNonScalar(type, sourceType))
                        info.matchType = match_fail;  // No translation from one non-scalar type to another
                    else
                    {
                        switch (type->getType())
                        {
                        case type_set:
                            if (binarySource)
                            {
                                if (type->queryChildType()->fieldType==sourceType->queryChildType()->fieldType &&
                                    type->queryChildType()->length==sourceType->queryChildType()->length)
                                    info.matchType = match_perfect;
                                else
                                    info.matchType = match_typecast;
                            }
                            else
                                info.matchType = match_typecast|match_dynamic;
                            break;
                        case type_row:      // These are not expected I think...
                            throwUnexpected();
                        case type_ifblock:
                        case type_record:
                        case type_table:
                        case type_dictionary:
                        {
                            const RtlRecord *subDest = destRecInfo.queryNested(idx);
                            const RtlRecord *subSrc = sourceRecInfo.queryNested(info.matchIdx);
                            info.subTrans = new GeneralRecordTranslator(*subDest, *subSrc, binarySource);
                            if (!info.subTrans->needsTranslate())
                            {
                                if (!binarySource)
                                    info.matchType = match_recurse|match_dynamic;
                                else
                                {
                                    // Child does not require translation, but check linkcount mode matches too!
                                    if (type->isLinkCounted())
                                    {
                                        if (sourceType->isLinkCounted())
                                            info.matchType = match_link;
                                        else
                                            info.matchType = match_recurse;
                                    }
                                    else
                                    {
                                        if (sourceType->isLinkCounted())
                                            info.matchType = match_recurse;
                                        else
                                            info.matchType = match_perfect;
                                    }
                                    if (info.matchType != match_recurse)
                                    {
                                        delete info.subTrans;
                                        info.subTrans = nullptr;
                                    }
                                }
                            }
                            else if (info.subTrans->canTranslate())
                            {
                                info.matchType = binarySource ? match_recurse : (match_recurse|match_dynamic);
                                unsigned childFlags = info.subTrans->matchFlags;
                                //Ignore differences in the keyed flag for child structures (it will be set later if this field is keyed)
                                matchFlags |= (FieldMatchType)(childFlags & ~match_keychange);
                            }
                            else
                                info.matchType = match_fail;
                            break;
                        }
                        case type_blob:
                            if (!binarySource)
                                info.matchType = match_fail;
                            else if (sourceType->isBlob())
                                info.matchType = match_perfect;  // We don't check that the child type matches
                            else
                                info.matchType = match_fail;
                            break;
                        default:
                            info.matchType = match_fail;
                            break;
                        }
                    }
                }
                else if (!binarySource)
                    info.matchType = match_typecast|match_dynamic;
                else if ((type->fieldType==sourceType->fieldType))
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
                                if (((sourceFlags|destFlags) & RFTMinifblock) == 0)
                                    fixedDelta += sourceType->getMinSize()-type->getMinSize();
#ifdef TRACE_TRANSLATION
                                DBGLOG("Increasing fixedDelta size by %d to %d for truncated field %d (%s)", sourceType->getMinSize()-type->getMinSize(), fixedDelta, idx, destRecInfo.queryName(idx));
#endif
                            }
                        }
                        else
                        {
                            if (type->canExtend(info.fillChar))
                            {
                                info.matchType = match_extend;
                                if (((sourceFlags|destFlags) & RFTMinifblock) == 0)
                                    fixedDelta += sourceType->getMinSize()-type->getMinSize();
#ifdef TRACE_TRANSLATION
                                DBGLOG("Decreasing fixedDelta size by %d to %d for truncated field %d (%s)", type->getMinSize()-sourceType->getMinSize(), fixedDelta, idx, destRecInfo.queryName(idx));
#endif
                            }
                        }
                    }
                }
                else if ((type->getType()==type_filepos || sourceType->getType()==type_filepos) &&
                         type->isUnsigned()==sourceType->isUnsigned())
                    info.matchType = match_filepos;
                else
                    info.matchType = match_typecast;
                if (deblob)
                    info.matchType |= match_deblob;
                if (sourceFlags & RFTMinifblock || field->flags & RFTMinifblock)
                    info.matchType |= match_inifblock;  // Avoids incorrect commoning up of adjacent matches
                // MORE - could note the highest interesting fieldnumber in the source and not bother filling in offsets after that
                // Not sure it would help much though - usually need to know the total record size anyway in real life
                if (idx != info.matchIdx)
                    matchFlags |= match_move;

                //Whether this field is in an ifblock, or needs to be copied by linking it do not count as changes
                FieldMatchType maskedType = (FieldMatchType)(info.matchType & ~(match_link|match_inifblock));
                if (((maskedType != match_perfect) || (idx != info.matchIdx)) && ((field->flags & RFTMispayloadfield) == 0 || (sourceFlags & RFTMispayloadfield) == 0))
                    matchFlags |= match_keychange;
                else if ((field->flags & RFTMispayloadfield) != (sourceFlags & RFTMispayloadfield))
                    matchFlags |= match_keychange;
            }
            matchFlags |= info.matchType;
        }
        if (sourceRecInfo.getNumFields() > destRecInfo.getNumFields()-defaulted)
        {
            matchFlags |= match_remove;
            for (unsigned idx = 0; idx < sourceRecInfo.getNumFields(); idx++)
            {
                const RtlFieldInfo *field = sourceRecInfo.queryField(idx);
                const char *name = sourceRecInfo.queryName(idx);
                if (destRecInfo.getFieldNum(name) == (unsigned) -1)
                {
                    // unmatched field
                    if ((field->flags & RFTMispayloadfield) == 0)
                        matchFlags |= match_keychange;
                    if (!destRecInfo.getFixedSize())
                    {
                        const RtlTypeInfo *type = field->type;
                        if (type->isFixedSize() && (field->flags & RFTMinifblock)==0)
                        {
#ifdef TRACE_TRANSLATION
                            DBGLOG("Reducing estimated size by %d for (fixed size) omitted field %s", (int) type->getMinSize(), field->name);
#endif
                            fixedDelta += type->getMinSize();
                        }
                        else
                            variableUnmatched.append(idx);
                    }
                    allUnmatched.append(idx);
                }
            }
#ifdef TRACE_TRANSLATION
            DBGLOG("Delta from fixed-size fields is %d bytes", fixedDelta);
#endif
        }
    }
    size32_t estimateNewSize(const RtlRow &sourceRow) const
    {
#ifdef TRACE_TRANSLATION
        DBGLOG("Source record size is %d", (int) sourceRow.getRecordSize());
#endif
        size32_t expectedSize = sourceRow.getRecordSize();
        assertex((int) expectedSize >= fixedDelta);
        expectedSize -= fixedDelta;
#ifdef TRACE_TRANSLATION
        DBGLOG("Source record size without fixed delta is %d", expectedSize);
#endif
        ForEachItemIn(i, variableUnmatched)
        {
            unsigned fieldNo = variableUnmatched.item(i);
            expectedSize -= sourceRow.getSize(fieldNo);
#ifdef TRACE_TRANSLATION
            DBGLOG("Reducing estimated size by %d to %d for omitted field %d (%s)", (int) sourceRow.getSize(fieldNo), expectedSize, fieldNo, sourceRecInfo.queryName(fieldNo));
#endif
        }
        if (matchFlags & ~(match_perfect|match_link|match_none|match_virtual|match_extend|match_truncate))
        {
            for (unsigned idx = 0; idx < destRecInfo.getNumFields(); idx++)
            {
                const MatchInfo &match = matchInfo[idx];
                const RtlTypeInfo *type = destRecInfo.queryType(idx);
                unsigned matchField = match.matchIdx;
                if ((match.matchType & match_inifblock) == 0)
                {
                    switch (match.matchType)
                    {
                    case match_perfect:
                    case match_link:
                    case match_none:
                    case match_virtual:
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
                        auto minSize = type->getMinSize();
                        auto sourceSize = sourceRow.getSize(matchField);
                        expectedSize += minSize;
                        assertex(expectedSize >= sourceSize);
                        expectedSize -= sourceSize;
    #ifdef TRACE_TRANSLATION
                        DBGLOG("Adjusting estimated size by (%d - %d) to %d for translated field %d (%s)", (int) sourceSize, minSize, expectedSize, matchField, sourceRecInfo.queryName(matchField));
    #endif
                        break;
                    }
                }
            }
        }
        return expectedSize;
    }
};

extern ECLRTL_API const IDynamicTransform *createRecordTranslator(const RtlRecord &destRecInfo, const RtlRecord &srcRecInfo)
{
    return new GeneralRecordTranslator(destRecInfo, srcRecInfo, true);
}

extern ECLRTL_API const IDynamicTransform *createRecordTranslatorViaCallback(const RtlRecord &destRecInfo, const RtlRecord &srcRecInfo, type_vals rawType)
{
    return new GeneralRecordTranslator(destRecInfo, srcRecInfo, false, rawType);
}

extern ECLRTL_API void throwTranslationError(const RtlRecord & destRecInfo, const RtlRecord & srcRecInfo, const char * filename)
{
    Owned<const IDynamicTransform> translator = createRecordTranslator(destRecInfo, srcRecInfo);
#ifdef _DEBUG
    translator->describe();
#endif
    if (!translator->canTranslate())
        throw MakeStringException(0, "Untranslatable record layout mismatch detected for: %s", filename);
    throw MakeStringException(0, "Translatable key layout mismatch reading file %s but translation disabled", filename);
}

class TranslatedRowStream : public CInterfaceOf<IRowStream>
{
public:
    TranslatedRowStream(IRowStream *_inputStream, IEngineRowAllocator *_resultAllocator, const RtlRecord &outputRecord, const RtlRecord &inputRecord)
    : inputStream(_inputStream), resultAllocator(_resultAllocator)
    {
        translator.setown(createRecordTranslator(outputRecord, inputRecord));
        translator->describe();
    }
    virtual const void *nextRow() override
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
        size32_t len = translator->translate(rowBuilder, fieldCallback, (const byte *) inRow);
        rtlReleaseRow(inRow);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop() override
    {
        resultAllocator.clear();
    }
    bool canTranslate() const
    {
        return translator->canTranslate();
    }
    bool needsTranslate() const
    {
        return translator->needsTranslate();
    }

    UnexpectedVirtualFieldCallback fieldCallback; // I'm not sure if an non unexpected callback can be implemented

protected:
    Linked<IRowStream> inputStream;
    Linked<IEngineRowAllocator> resultAllocator;
    Owned<const IDynamicTransform> translator;
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

// A key translator allows us to transform a RowFilter that refers to src to one that refers to dest.
// Basically just a map of those fields with matching types.

class CKeyTranslator : public CInterfaceOf<IKeyTranslator>
{
public:
    CKeyTranslator(const RtlRecord &actual, const RtlRecord &expected)
    {
        translateNeeded = false;
        for (unsigned expectedIdx = 0; expectedIdx < expected.getNumFields(); expectedIdx++)
        {
            unsigned actualIdx = actual.getFieldNum(expected.queryName(expectedIdx));
            if (actualIdx != (unsigned) -1)
            {
                const RtlTypeInfo *expectedType = expected.queryType(expectedIdx);
                const RtlTypeInfo *actualType = actual.queryType(actualIdx);
                if (!actualType->equivalent(expectedType))
                    actualIdx = (unsigned) -2;
            }
            map.append(actualIdx);
            if (actualIdx != expectedIdx)
                translateNeeded = true;
        }
    }
    virtual void describe() const override
    {
        ForEachItemIn(idx, map)
        {
            unsigned mapped = map.item(idx);
            switch (mapped)
            {
            case (unsigned) -1: DBGLOG("No match for field %d", idx); break;
            case (unsigned) -2: DBGLOG("Incompatible field match for field %d", idx); break;
            default: DBGLOG("keyed field %d can map to field %d", idx, mapped); break;
            }
        }
    }
    virtual bool translate(RowFilter &filters) const override
    {
        bool mapNeeded = false;
        if (translateNeeded)
        {
            unsigned numFields = filters.numFilterFields();
            for (unsigned idx = 0; idx < numFields; idx++)
            {
                unsigned fieldNum = filters.queryFilter(idx).queryFieldIndex();
                unsigned mappedFieldNum = map.isItem(fieldNum) ? map.item(fieldNum) : (unsigned) -1;
                if (mappedFieldNum != fieldNum)
                {
                    mapNeeded = true;
                    switch (mappedFieldNum)
                    {
                    case (unsigned) -1: throw makeStringExceptionV(0, "Cannot translate keyed filter on field %u - no matching field", idx);
                    case (unsigned) -2: throw makeStringExceptionV(0, "Cannot translate keyed filter on field %u - incompatible matching field type", idx);
                    default:
                        filters.remapField(idx, mappedFieldNum);
                        break;
                    }
                }
            }
            if (mapNeeded)
                filters.recalcFieldsRequired();
        }
        return mapNeeded;
    }
    virtual bool translate(RowFilter &filter, IConstArrayOf<IFieldFilter> &in) const override
    {
        bool mapNeeded = false;
        if (translateNeeded)
        {
            unsigned numFields = in.length();
            for (unsigned idx = 0; idx < numFields; idx++)
            {
                unsigned fieldNum = in.item(idx).queryFieldIndex();
                unsigned mappedFieldNum = map.isItem(fieldNum) ? map.item(fieldNum) : (unsigned) -1;
                if (mappedFieldNum != fieldNum)
                {
                    mapNeeded = true;
                    switch (mappedFieldNum)
                    {
                    case (unsigned) -1: throw makeStringExceptionV(0, "Cannot translate keyed filter on field %u - no matching field", idx);
                    case (unsigned) -2: throw makeStringExceptionV(0, "Cannot translate keyed filter on field %u - incompatible matching field type", idx);
                    default:
                        filter.addFilter(*in.item(idx).remap(mappedFieldNum));
                        break;
                    }
                }
                else
                    filter.addFilter(OLINK(in.item(idx)));
            }
        }
        return mapNeeded;
    }
    virtual bool needsTranslate() const
    {
        return translateNeeded;
    }
protected:
    UnsignedArray map;
    bool translateNeeded = false;
};

extern ECLRTL_API const IKeyTranslator *createKeyTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo)
{
    return new CKeyTranslator(_destRecInfo, _srcRecInfo);
}


//---------------------------------------------------------------------------------------------------------------------

const char * NullVirtualFieldCallback::queryLogicalFilename(const void * row)
{
    return "";
}

unsigned __int64 NullVirtualFieldCallback::getFilePosition(const void * row)
{
    return 0;
}

unsigned __int64 NullVirtualFieldCallback::getLocalFilePosition(const void * row)
{
    return 0;
}

const byte * NullVirtualFieldCallback::lookupBlob(unsigned __int64 id)
{
    return nullptr;
}

const char * UnexpectedVirtualFieldCallback::queryLogicalFilename(const void * row)
{
    throwUnexpectedX("VIRTUAL(LOGICALFILENAME)");
}

unsigned __int64 UnexpectedVirtualFieldCallback::getFilePosition(const void * row)
{
    throwUnexpectedX("VIRTUAL(FILEPOSITION)");
}

unsigned __int64 UnexpectedVirtualFieldCallback::getLocalFilePosition(const void * row)
{
    throwUnexpectedX("VIRTUAL(LOCALFILEPOSITION)");
}

const byte * UnexpectedVirtualFieldCallback::lookupBlob(unsigned __int64 id)
{
    throwUnexpectedX("BLOB");
}

unsigned __int64 FetchVirtualFieldCallback::getFilePosition(const void * row)
{
    return filepos;
}

const char * LocalVirtualFieldCallback::queryLogicalFilename(const void * row)
{
    return filename;
}

unsigned __int64 LocalVirtualFieldCallback::getFilePosition(const void * row)
{
    return filepos;
}

unsigned __int64 LocalVirtualFieldCallback::getLocalFilePosition(const void * row)
{
    return localfilepos;
}

const byte * LocalVirtualFieldCallback::lookupBlob(unsigned __int64 id)
{
    throwUnexpectedX("BLOB");
}

