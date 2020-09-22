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
#include "rtlnewkey.hpp"

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
    const IFieldFilter * filter = nullptr;

    const RtlTypeInfo *createRtlTypeInfo() const;
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
    virtual const RtlTypeInfo *addType(FieldTypeInfoStruct &info, const IInterface *typeOrIfblock) = 0;
    /*
     * Check if a type has already been created for a given key
     *
     * @param key  A unique pointer used to dedup typeinfo structures
     * @return     RtlTypeInfo structure, or nullptr if not yet created
     */
    virtual const RtlTypeInfo *lookupType(const IInterface *key) const = 0;
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

enum class RecordTranslationMode : byte
{
    None = 0,       // Never translate - throw an error if the ecl does not match published
    // All = 1,     // Translate all fields. Not supported or used
    Payload = 2,    // Translate all fields in datasets, and only payload fields in indexes
    AlwaysDisk = 3, // Always translate - even if wouldn't normally (e.g. csv/xml source read as binary), or crcs happen to match
    AlwaysECL = 4,  // Ignore the published format - can make sense to force no translation e.g. when field names have changed
    Unspecified = 5
};  // AlwaysDisk and AlwaysECL are for testing purposes only, and can only be set per file (not globally)

extern ECLRTL_API RecordTranslationMode getTranslationMode(const char *modeStr, bool isLocal);
extern ECLRTL_API const char *getTranslationModeText(RecordTranslationMode val);

interface IDynamicRowIterator;
interface IDynamicFieldValueFetcher : extends IInterface
{
    virtual const byte *queryValue(unsigned fieldNum, size_t &sz) const = 0;
    virtual IDynamicRowIterator *getNestedIterator(unsigned fieldNum) const = 0;
    virtual size_t getSize(unsigned fieldNum) const = 0;
    virtual size32_t getRecordSize() const = 0;
};

interface IDynamicRowIterator : extends IIteratorOf<IDynamicFieldValueFetcher>
{
};

interface IDynamicTransform : public IInterface
{
    virtual void describe() const = 0;
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const byte *sourceRec) const = 0;
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const RtlRow &sourceRow) const = 0;
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const IDynamicFieldValueFetcher & fetcher) const = 0;
    virtual bool canTranslate() const = 0;
    virtual bool needsTranslate() const = 0;
    virtual bool keyedTranslated() const = 0;
    virtual bool needsNonVirtualTranslate() const = 0;
};

interface IKeyTranslator : public IInterface
{
    /*
     * Describe the operations of a translator to the log
     */
    virtual void describe() const = 0;
    /*
     * Translate the field numbers of a RowFilter array in-situ. An exception will be thrown if an
     * untranslatable field is encountered.
     *
     * @param filters  The RowFilter array to be updated
     * @return         Indicates whether any field numbers changed
     */
    virtual bool translate(RowFilter &filters) const = 0;
    /*
     * Translate the field numbers of a RowFilter array, creating a new RowFilter array. An exception will be thrown if an
     * untranslatable field is encountered.
     *
     * @param filters  The input RowFilter array to be translated
     * @param result   Updated to add translated versions of all the FieldFilters in the input
     * @return         Indicates whether any field numbers changed
     */
    virtual bool translate(RowFilter &result, IConstArrayOf<IFieldFilter> &filters) const = 0;
    /*
     * Return whether any fields are translated by this translator
     */
    virtual bool needsTranslate() const = 0;
};

interface IDynamicTransformViaCallback : public IInterface
{
    virtual void describe() const = 0;
    virtual size32_t translate(ARowBuilder &builder, IVirtualFieldCallback & callback, const void *sourceRec) const = 0;
    virtual bool canTranslate() const = 0;
    virtual bool needsTranslate() const = 0;
    virtual bool keyedTranslated() const = 0;
    virtual bool needsNonVirtualTranslate() const = 0;
};

extern ECLRTL_API const IDynamicTransform *createRecordTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo);
extern ECLRTL_API const IDynamicTransform *createRecordTranslatorViaCallback(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo, type_vals rawType);
extern ECLRTL_API void throwTranslationError(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo, const char * filename);

extern ECLRTL_API const IKeyTranslator *createKeyTranslator(const RtlRecord &_destRecInfo, const RtlRecord &_srcRecInfo);

extern ECLRTL_API IRtlFieldTypeDeserializer *createRtlFieldTypeDeserializer();

extern ECLRTL_API StringBuffer &dumpTypeInfo(StringBuffer &ret, const RtlTypeInfo *t);

extern ECLRTL_API bool dumpTypeInfo(MemoryBuffer &ret, const RtlTypeInfo *t);

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

//---------------------------------------------------------------------------------------------------------------------

//Default implementations of the virtual field callbacks
class ECLRTL_API NullVirtualFieldCallback : public CInterfaceOf<IVirtualFieldCallback>
{
public:
    virtual const char * queryLogicalFilename(const void * row) override;
    virtual unsigned __int64 getFilePosition(const void * row) override;
    virtual unsigned __int64 getLocalFilePosition(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override;
};

class ECLRTL_API UnexpectedVirtualFieldCallback : public CInterfaceOf<IVirtualFieldCallback>
{
public:
    virtual const char * queryLogicalFilename(const void * row) override;
    virtual unsigned __int64 getFilePosition(const void * row) override;
    virtual unsigned __int64 getLocalFilePosition(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override;

};

typedef UnexpectedVirtualFieldCallback IndexVirtualFieldCallback;

//Backward compatibility implementation for fetch - only fileposition implemented.  Revisit later when all working.
class ECLRTL_API FetchVirtualFieldCallback : public UnexpectedVirtualFieldCallback
{
public:
    FetchVirtualFieldCallback(unsigned __int64 _filepos) : filepos(_filepos) {}

    virtual unsigned __int64 getFilePosition(const void * row) override;
private:
    unsigned __int64 filepos;
};

class ECLRTL_API LocalVirtualFieldCallback : public CInterfaceOf<IVirtualFieldCallback>
{
public:
    LocalVirtualFieldCallback(const char * _filename, unsigned __int64 _filepos, unsigned __int64 _localfilepos)
    : filename(_filename), filepos(_filepos), localfilepos(_localfilepos) {}

    virtual const char * queryLogicalFilename(const void * row) override;
    virtual unsigned __int64 getFilePosition(const void * row) override;
    virtual unsigned __int64 getLocalFilePosition(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override;
private:
    const char * filename;
    unsigned __int64 filepos;
    unsigned __int64 localfilepos;
};

#endif
