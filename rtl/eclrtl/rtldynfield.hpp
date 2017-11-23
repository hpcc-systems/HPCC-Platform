/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef rtldynfield_hpp
#define rtldynfield_hpp

#include "rtlfield.hpp"

//These classes support the dynamic creation of type and field information

//-------------------------------------------------------------------------------------------------------------------

/*
 * Used to represent the info that is needed to dynamically create an RtlTypeInfo object
 */
struct ECLRTL_API FieldTypeInfoStruct
{
public:
    unsigned fieldType = 0;
    unsigned length = 0;
    const char *locale = nullptr;
    const char *className = nullptr;
    const RtlTypeInfo *childType = nullptr;
    const RtlFieldInfo * * fieldsArray = nullptr;

    const RtlTypeInfo *createRtlTypeInfo(IThorIndexCallback *_callback) const;
};

interface ITypeInfo;

/**
 *   IRtlFieldTypeDeserializer is used to manage the creation of RtlTypeInfo structures dynamically.
 *   All created structures are owned by the deserializer and will be destroyed when the deserializer is destroyed.
 */
interface IRtlFieldTypeDeserializer : public IInterface
{
    /*
     * Create RtlTypeInfo structures from a serialized json representation
     *
     * @param json The json representation
     * @return     Deserialized RtlTypeInfo structure
     */
    virtual const RtlTypeInfo *deserialize(const char *json) = 0;

    /*
     * Create RtlTypeInfo structures from a serialized json representation
     *
     * @param jsonTree The json representation
     * @return         Deserialized RtlTypeInfo structure
     */
     virtual const RtlTypeInfo *deserialize(IPropertyTree &jsonTree) = 0;

    /*
     * Create RtlTypeInfo structures from a serialized binary representation
     *
     * @param buf  The binary representation
     * @return     Deserialized RtlTypeInfo structure
     */
    virtual const RtlTypeInfo *deserialize(MemoryBuffer &buf) = 0;

    /*
     * Create a single RtlTypeInfo structure from a FieldTypeInfoStruct
     *
     * @param info The information used to create the type
     * @param key  A unique pointer used to dedup typeinfo structures
     * @return     RtlTypeInfo structure
     */
    virtual const RtlTypeInfo *addType(FieldTypeInfoStruct &info, const ITypeInfo *key) = 0;
    /*
     * Check if a type has already been created for a given key
     *
     * @param key  A unique pointer used to dedup typeinfo structures
     * @return     RtlTypeInfo structure, or nullptr if not yet created
     */
    virtual const RtlTypeInfo *lookupType(const ITypeInfo *key) const = 0;
    /*
     * Create RtlFieldInfo structure as part of a RtlTypeInfo tree
     *
     * @param fieldName Field name
     * @param xpath     XPath
     * @param type      Field type
     * @param flags     Field flags
     * @param init      Field initializer, or nullptr
     * @return          RtlFieldInfo structure. All strings will be owned by the deserializer object
     */
    virtual const RtlFieldInfo *addFieldInfo(const char *fieldName, const char *xpath, const RtlTypeInfo *type, unsigned flags, const char *init) = 0;

};

interface IDynamicTransform : public IInterface
{
    virtual void describe() const = 0;
    virtual size32_t translate(ARowBuilder &builder, const byte *sourceRec) const = 0;
    virtual bool canTranslate() const = 0;
    virtual bool needsTranslate() const = 0;
};

extern ECLRTL_API const IDynamicTransform *createRecordTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo);

extern ECLRTL_API IRtlFieldTypeDeserializer *createRtlFieldTypeDeserializer(IThorIndexCallback *callback);

extern ECLRTL_API StringBuffer &dumpTypeInfo(StringBuffer &ret, const RtlTypeInfo *t);

extern ECLRTL_API MemoryBuffer &dumpTypeInfo(MemoryBuffer &ret, const RtlTypeInfo *t);

/**
 * Serialize metadata of supplied record to JSON, and return it to ECL caller as a string. Used for testing serializer.
 *
 */
extern ECLRTL_API void dumpRecordType(size32_t & __lenResult, char * & __result, IOutputMetaData &  metaVal);

/**
 * Serialize metadata of supplied record to DATA.
 *
 */
extern ECLRTL_API void serializeRecordType(size32_t & __lenResult, void * & __result, IOutputMetaData &  metaVal);

/**
 * Extract a field from a record via dynamic column number
 *
 */
extern ECLRTL_API void getFieldVal(size32_t & __lenResult, char * & __result, int column, IOutputMetaData &  metaVal, const byte *row);

/**
 * Extract a column number from a record via dynamic fieldname
 *
 */
extern ECLRTL_API int getFieldNum(const char *fieldName, IOutputMetaData &  metaVal);

extern ECLRTL_API IRowStream * transformRecord(IEngineRowAllocator * resultAllocator,IOutputMetaData &  metaInput,IRowStream * input);

#endif
