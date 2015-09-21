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
#include "v8.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield_imp.hpp"
#include "nbcd.hpp"
#include "roxiemem.hpp"
#include <vector>

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] = {
    "V8 JavaScript Embed Helper 1.0.0",
    NULL };

static const char *version = "V8 JavaScript Embed Helper 1.0.0";

extern "C" EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = version;
    pb->moduleName = "javascript";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "V8 JavaScript Embed Helper";
    return true;
}

static void UNSUPPORTED(const char *feature) __attribute__((noreturn));

static void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in v8embed plugin", feature);
}

static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("v8embed: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", str(field->name));
    rtlFail(0, msg.str());
}

namespace javascriptLanguageHelper {

// A JSRowBuilder object is used to construct an ECL row from a javascript object

class JSRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    JSRowBuilder(v8::Local<v8::Object> _row, const RtlFieldInfo *_outerRow)
    : row(_row), outerRow(_outerRow), named(true), idx(0)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        return nextField(field)->BooleanValue();
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        UNIMPLEMENTED;
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        return v8::Number::Cast(*nextField(field))->Value();
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        return v8::Integer::Cast(*nextField(field))->Value();
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        return v8::Integer::Cast(*nextField(field))->Value();
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        v8::String::AsciiValue ascii(nextField(field));
        rtlStrToStrX(chars, result, ascii.length(), *ascii);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        v8::Local<v8::Value> value = nextField(field);
        if (!value->IsString())
            typeError("string", field);
        v8::String::Utf8Value utf8(value);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUtf8X(chars, result, numchars, *utf8);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        v8::Local<v8::Value> value = nextField(field);
        if (!value->IsString())
            typeError("string", field);
        v8::String::Utf8Value utf8(value);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUnicodeX(chars, result, numchars, *utf8);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        value.setReal(getRealResult(field));
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false;
        v8::Local<v8::Value> value = nextField(field);
        if (!value->IsArray())
            typeError("array", field);
        push(false);
        row = v8::Array::Cast(*value);
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        assertex(!named);
        return row->Has(idx);
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        v8::Local<v8::Value> value = nextField(field);
        if (!value->IsArray())
            typeError("array", field);
        push(false);
        row = v8::Array::Cast(*value);
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            v8::Local<v8::Value> value = nextField(field);
            if (!value->IsObject())
                typeError("object", field);
            push(true);
            row = v8::Object::Cast(*value);
        }
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        assertex(!named);
        return row->Has(idx);
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        pop();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        pop();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
            pop();
    }
protected:
    void pop()
    {
        named = namedStack.popGet();
        idx = idxStack.popGet();
        row = stack.back();
        stack.pop_back();
    }
    void push(bool _named)
    {
        namedStack.append(named);
        idxStack.append(idx);
        stack.push_back(row);
        named = _named;
        idx = 0;
    }
    v8::Local<v8::Value> nextField(const RtlFieldInfo * field)
    {
        v8::Local<v8::Value> v;
        if (named)
        {
            v8::Local<v8::String> name = v8::String::New(str(field->name));
            if (!row->Has(name))
            {
                VStringBuffer msg("v8embed: No value for field %s", str(field->name));
                rtlFail(0, msg.str());
            }
            v = row->Get(name);
        }
        else
        {
            assertex(row->Has(idx));  // Logic in processNextXXX should have ensured
            v = row->Get(idx++);
        }
        return v;
    }
    v8::Local<v8::Object> row; // current row, set, or dataset...
    std::vector< v8::Local<v8::Object> > stack;
    IntArray idxStack;
    BoolArray namedStack;
    const RtlFieldInfo *outerRow;
    int idx;
    bool named;
};

// A JSObjectBuilder object is used to construct a JS Object from an ECL row

class JSObjectBuilder : public CInterfaceOf<IFieldProcessor>
{
public:
    JSObjectBuilder(const RtlFieldInfo *_outerRow)
    : outerRow(_outerRow), idx(0), inDataset(false)
    {
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t utfCharCount;
        rtlDataAttr utfText;
        rtlStrToUtf8X(utfCharCount, utfText.refstr(), len, value);
        processUtf8(utfCharCount, utfText.getstr(), field);
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        addProp(field, v8::Boolean::New(value));
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        v8::Local<v8::Array> array = v8::Array::New(len);
        const byte *vval = (const byte *) value;
        for (int i = 0; i < len; i++)
        {
            array->Set(v8::Number::New(i), v8::Integer::New(vval[i])); // feels horridly inefficient, but seems to be the expected approach
        }
        addProp(field, array);
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        addProp(field, v8::Integer::New(value));
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        addProp(field, v8::Integer::NewFromUnsigned(value));
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        addProp(field, v8::Number::New(value));
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setDecimal(digits, precision, value);
        addProp(field, v8::Number::New(val.getReal()));
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setUDecimal(digits, precision, value);
        addProp(field, v8::Number::New(val.getReal()));
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        addProp(field, v8::String::New(value, len));
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        addProp(field, v8::String::New(value, rtlUtf8Size(len, value)));
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        push();
        inDataset = true;
        if (isAll)
            rtlFail(0, "v8embed: ALL sets are not supported");

        obj = v8::Array::New();
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        push();
        inDataset = true;
        obj = v8::Array::New();
        return true;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
            push();
        obj = v8::Object::New();
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        pop(field);
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        pop(field);
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            pop(field);
        }
    }
    v8::Local<v8::Object> getObject()
    {
        return obj;
    }
protected:
    void push()
    {
        idxStack.append(idx);
        stack.push_back(obj);
        dsStack.append(inDataset);
        inDataset = false;
        idx = 0;
        obj.Clear();
    }
    void pop(const RtlFieldInfo * field)
    {
        inDataset = dsStack.popGet();
        idx = idxStack.popGet();
        v8::Local<v8::Object> row = obj;
        obj = stack.back();
        stack.pop_back();
        addProp(field, row);
    }
    void addProp(const RtlFieldInfo * field, v8::Handle<v8::Value> value)
    {
        assertex(!obj.IsEmpty());
        if (inDataset)
            obj->Set(idx++, value);
        else
            obj->Set(v8::String::New(str(field->name)), value);
    }
    v8::Local<v8::Object> obj;
    std::vector< v8::Local<v8::Object> > stack;
    const RtlFieldInfo *outerRow;
    BoolArray dsStack;
    IntArray idxStack;
    int idx;
    bool inDataset;
};

static size32_t getRowResult(v8::Handle<v8::Value> result, ARowBuilder &builder)
{
    if (result.IsEmpty() || !result->IsObject())
        typeError("object", NULL);
    v8::HandleScope scope;  // Probably not needed
    v8::Local<v8::Object> row = v8::Object::Cast(*result);
    const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
    assertex(typeInfo);
    RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
    JSRowBuilder jsRowBuilder(row, &dummyField);
    return typeInfo->build(builder, 0, &dummyField, jsRowBuilder);
}

// An embedded javascript function that returns a dataset will return a JSRowStream object that can be
// interrogated to return each row of the result in turn

class JSRowStream : public CInterfaceOf<IRowStream>
{
public:
    JSRowStream(v8::Handle<v8::Value> _result, IEngineRowAllocator *_resultAllocator)
    : rowIdx(0), resultAllocator(_resultAllocator)
    {
        if (_result.IsEmpty() || !_result->IsArray())
            typeError("array", NULL);
        result = v8::Persistent<v8::Array>(v8::Array::Cast(*_result));
    }
    ~JSRowStream()
    {
        result.Dispose();
    }
    virtual const void *nextRow()
    {
        if (result.IsEmpty())
            return NULL;
        v8::HandleScope scope;
        if (!result->Has(rowIdx))
        {
            stop();
            return NULL;
        }
        v8::Local<v8::Value> row = result->Get(rowIdx);
        rowIdx++;
        if (!row->IsObject())
            typeError("object", NULL);
        v8::Local<v8::Object> rowObject = v8::Object::Cast(*row);
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        size32_t len = javascriptLanguageHelper::getRowResult(rowObject, rowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
        result.Clear();
    }

protected:
    Linked<IEngineRowAllocator> resultAllocator;
    unsigned rowIdx;
    v8::Persistent<v8::Array> result;
};

class V8JavascriptEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    V8JavascriptEmbedFunctionContext()
    {
        isolate = v8::Isolate::New();
        isolate->Enter();
        context = v8::Context::New();
        context->Enter();
    }
    ~V8JavascriptEmbedFunctionContext()
    {
        script.Dispose();
        result.Dispose();
        context->Exit();
        context.Dispose();
        isolate->Exit();
        isolate->Dispose();
    }
    virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
    {
        return NULL;
    }
    virtual void paramWriterCommit(IInterface *writer)
    {
    }
    virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
    {
    }


    virtual void bindBooleanParam(const char *name, bool val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Boolean::New(val));
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        v8::HandleScope handle_scope;
        v8::Local<v8::Array> array = v8::Array::New(len);
        const byte *vval = (const byte *) val;
        for (int i = 0; i < len; i++)
        {
            array->Set(v8::Number::New(i), v8::Integer::New(vval[i])); // feels horridly inefficient, but seems to be the expected approach
        }
        context->Global()->Set(v8::String::New(name), array);
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Number::New(val));
    }
    virtual void bindRealParam(const char *name, double val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Number::New(val));
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        bindSignedParam(name, val);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        // MORE - might need to check does not overflow 32 bits? Or store as a real?
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Integer::New(val));
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        bindUnsignedParam(name, val);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        // MORE - might need to check does not overflow 32 bits
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Integer::NewFromUnsigned(val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        size32_t utfCharCount;
        rtlDataAttr utfText;
        rtlStrToUtf8X(utfCharCount, utfText.refstr(), len, val);
        bindUTF8Param(name, utfCharCount, utfText.getstr());
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val, rtlUtf8Size(chars, val)));
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val, chars));
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        if (isAll)
            rtlFail(0, "v8embed: Cannot pass ALL");
        v8::HandleScope handle_scope;
        type_t typecode = (type_t) elemType;
        const byte *inData = (const byte *) setData;
        const byte *endData = inData + totalBytes;
        int numElems;
        if (elemSize == UNKNOWN_LENGTH)
        {
            numElems = 0;
            // Will need 2 passes to work out how many elements there are in the set :(
            while (inData < endData)
            {
                int thisSize;
                switch (elemType)
                {
                case type_varstring:
                    thisSize = strlen((const char *) inData) + 1;
                    break;
                case type_string:
                    thisSize = * (size32_t *) inData + sizeof(size32_t);
                    break;
                case type_unicode:
                    thisSize = (* (size32_t *) inData) * sizeof(UChar) + sizeof(size32_t);
                    break;
                case type_utf8:
                    thisSize = rtlUtf8Size(* (size32_t *) inData, inData + sizeof(size32_t)) + sizeof(size32_t);
                    break;
                default:
                    rtlFail(0, "v8embed: Unsupported parameter type");
                    break;
                }
                inData += thisSize;
                numElems++;
            }
            inData = (const byte *) setData;
        }
        else
            numElems = totalBytes / elemSize;
        v8::Local<v8::Array> array = v8::Array::New(numElems);
        v8::Handle<v8::Value> thisItem;
        size32_t thisSize = elemSize;
        for (int idx = 0; idx < numElems; idx++)
        {
            switch (typecode)
            {
            case type_int:
                thisItem = v8::Integer::New(rtlReadInt(inData, elemSize));
                break;
            case type_unsigned:
                thisItem = v8::Integer::NewFromUnsigned(rtlReadUInt(inData, elemSize));
                break;
            case type_varstring:
            {
                size32_t numChars = strlen((const char *) inData);
                size32_t utfCharCount;
                rtlDataAttr utfText;
                rtlStrToUtf8X(utfCharCount, utfText.refstr(), numChars, (const char *) inData);
                thisItem = v8::String::New(utfText.getstr(), rtlUtf8Size(utfCharCount, utfText.getstr()));
                if (elemSize == UNKNOWN_LENGTH)
                    thisSize = numChars + 1;
                break;
            }
            case type_string:
            {
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = * (size32_t *) inData;
                    inData += sizeof(size32_t);
                }
                size32_t utfCharCount;
                rtlDataAttr utfText;
                rtlStrToUtf8X(utfCharCount, utfText.refstr(), thisSize, (const char *) inData);
                thisItem = v8::String::New(utfText.getstr(), rtlUtf8Size(utfCharCount, utfText.getstr()));
                break;
            }
            case type_real:
                if (elemSize == sizeof(double))
                    thisItem = v8::Number::New(* (double *) inData);
                else
                    thisItem = v8::Number::New(* (float *) inData);
                break;
            case type_boolean:
                assertex(elemSize == sizeof(bool));
                thisItem = v8::Boolean::New(* (bool *) inData);
                break;
            case type_unicode:
            {
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = (* (size32_t *) inData) * sizeof(UChar); // NOTE - it's in chars...
                    inData += sizeof(size32_t);
                }
                thisItem = v8::String::New((const UChar *) inData, thisSize/sizeof(UChar));
                break;
            }
            case type_utf8:
            {
                assertex (elemSize == UNKNOWN_LENGTH);
                size32_t numChars = * (size32_t *) inData;
                inData += sizeof(size32_t);
                thisSize = rtlUtf8Size(numChars, inData);
                thisItem = v8::String::New((const char *) inData, thisSize);
                break;
            }
            default:
                rtlFail(0, "v8embed: Unsupported parameter type");
                break;
            }
            inData += thisSize;
            array->Set(v8::Number::New(idx), thisItem);
        }
        context->Global()->Set(v8::String::New(name), array);
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        v8::HandleScope handle_scope;
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        JSObjectBuilder objBuilder(&dummyField);
        typeInfo->process(val, val, &dummyField, objBuilder); // Creates a JS object from the incoming ECL row
        context->Global()->Set(v8::String::New(name), objBuilder.getObject());
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        v8::HandleScope handle_scope;
        v8::Local<v8::Array> array = v8::Array::New();
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        int idx = 0;
        loop
        {
            roxiemem::OwnedConstRoxieRow row = val->ungroupedNextRow();
            if (!row)
                break;
            JSObjectBuilder objBuilder(&dummyField);
            const byte *brow = (const byte *) row.get();
            typeInfo->process(brow, brow, &dummyField, objBuilder); // Creates a JS object from the incoming ECL row
            array->Set(idx++, objBuilder.getObject());
        }
        context->Global()->Set(v8::String::New(name), array);
    }

    virtual bool getBooleanResult()
    {
        assertex (!result.IsEmpty());
        return result->BooleanValue();
    }
    virtual void getDataResult(size32_t &__len, void * &__result)
    {
        assertex (!result.IsEmpty() && result->IsArray());
        v8::HandleScope handle_scope;
        v8::Handle<v8::Array> array = v8::Handle<v8::Array>::Cast(result);
        __len = array->Length();
        __result = rtlMalloc(__len);
        byte *bresult = (byte *) __result;
        for (size32_t i = 0; i < __len; i++)
        {
            bresult[i] = v8::Integer::Cast(*array->Get(i))->Value(); // feels horridly inefficient, but seems to be the expected approach
        }
    }
    virtual double getRealResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Number::Cast(*result)->Value();
    }
    virtual __int64 getSignedResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Integer::Cast(*result)->Value();
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Integer::Cast(*result)->Value();
    }
    virtual void getStringResult(size32_t &__chars, char * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::AsciiValue ascii(result);
        rtlStrToStrX(__chars, __result, ascii.length(), *ascii);
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::Utf8Value utf8(result);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUtf8X(__chars, __result, numchars, *utf8);
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::Utf8Value utf8(result);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUnicodeX(__chars, __result, numchars, *utf8);
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        assertex (!result.IsEmpty());
        if (!result->IsArray())
            rtlFail(0, "v8embed: type mismatch - return value was not an array");
        v8::HandleScope handle_scope;
        v8::Handle<v8::Array> array = v8::Handle<v8::Array>::Cast(result);
        size_t numResults = array->Length();
        rtlRowBuilder out;
        byte *outData = NULL;
        size32_t outBytes = 0;
        if (elemSize != UNKNOWN_LENGTH)
        {
            out.ensureAvailable(numResults * elemSize); // MORE - check for overflow?
            outData = out.getbytes();
        }
        for (int i = 0; i < numResults; i++)
        {
            v8::Local<v8::Value> elem = array->Get(i);
            if (elem.IsEmpty())
                rtlFail(0, "v8embed: type mismatch - empty value in returned array");
            switch ((type_t) elemType)
            {
            case type_int:
                rtlWriteInt(outData, v8::Integer::Cast(*elem)->Value(), elemSize);
                break;
            case type_unsigned:
                rtlWriteInt(outData, v8::Integer::Cast(*elem)->Value(), elemSize);
                break;
            case type_real:
                if (elemSize == sizeof(double))
                    * (double *) outData = (double) v8::Number::Cast(*elem)->Value();
                else
                {
                    assertex(elemSize == sizeof(float));
                    * (float *) outData = (float) v8::Number::Cast(*elem)->Value();
                }
                break;
            case type_boolean:
                assertex(elemSize == sizeof(bool));
                * (bool *) outData = elem->BooleanValue();
                break;
            case type_string:
            case type_varstring:
            {
                if (!elem->IsString())
                    rtlFail(0, "v8embed: type mismatch - return value in list was not a STRING");
                v8::String::AsciiValue ascii(elem);
                const char * text =  *ascii;
                size_t lenBytes = ascii.length();
                if (elemSize == UNKNOWN_LENGTH)
                {
                    if (elemType == type_string)
                    {
                        out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                        outData = out.getbytes() + outBytes;
                        * (size32_t *) outData = lenBytes;
                        rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                        outBytes += lenBytes + sizeof(size32_t);
                    }
                    else
                    {
                        out.ensureAvailable(outBytes + lenBytes + 1);
                        outData = out.getbytes() + outBytes;
                        rtlStrToVStr(0, outData, lenBytes, text);
                        outBytes += lenBytes + 1;
                    }
                }
                else
                {
                    if (elemType == type_string)
                        rtlStrToStr(elemSize, outData, lenBytes, text);
                    else
                        rtlStrToVStr(elemSize, outData, lenBytes, text);  // Fixed size null terminated strings... weird.
                }
                break;
            }
            case type_unicode:
            case type_utf8:
            {
                if (!elem->IsString())
                    rtlFail(0, "v8embed: type mismatch - return value in list was not a STRING");
                v8::String::Utf8Value utf8(elem);
                size_t lenBytes = utf8.length();
                const char * text =  *utf8;
                size32_t numchars = rtlUtf8Length(lenBytes, text);
                if (elemType == type_utf8)
                {
                    assertex (elemSize == UNKNOWN_LENGTH);
                    out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                    outData = out.getbytes() + outBytes;
                    * (size32_t *) outData = numchars;
                    rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                    outBytes += lenBytes + sizeof(size32_t);
                }
                else
                {
                    if (elemSize == UNKNOWN_LENGTH)
                    {
                        out.ensureAvailable(outBytes + numchars*sizeof(UChar) + sizeof(size32_t));
                        outData = out.getbytes() + outBytes;
                        // You can't assume that number of chars in utf8 matches number in unicode16 ...
                        size32_t numchars16;
                        rtlDataAttr unicode16;
                        rtlUtf8ToUnicodeX(numchars16, unicode16.refustr(), numchars, text);
                        * (size32_t *) outData = numchars16;
                        rtlUnicodeToUnicode(numchars16, (UChar *) (outData+sizeof(size32_t)), numchars16, unicode16.getustr());
                        outBytes += numchars16*sizeof(UChar) + sizeof(size32_t);
                    }
                    else
                        rtlUtf8ToUnicode(elemSize / sizeof(UChar), (UChar *) outData, numchars, text);
                }
                break;
            }
            default:
                rtlFail(0, "v8embed: type mismatch - unsupported return type");
            }
            if (elemSize != UNKNOWN_LENGTH)
            {
                outData += elemSize;
                outBytes += elemSize;
            }
        }
        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }

    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new JSRowStream(result, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        size32_t len = javascriptLanguageHelper::getRowResult(result, rowBuilder);
        return (byte *) rowBuilder.finalizeRowClear(len);
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        return javascriptLanguageHelper::getRowResult(result, builder);
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        v8::HandleScope handle_scope;
        v8::Handle<v8::String> source = v8::String::New(utf, rtlUtf8Size(lenChars, utf));
        v8::Handle<v8::Script> lscript = v8::Script::Compile(source);
        script = v8::Persistent<v8::Script>::New(lscript);
    }
    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        UNIMPLEMENTED; // Not sure if meaningful for js
    }
    virtual void callFunction()
    {
        assertex (!script.IsEmpty());
        v8::HandleScope handle_scope;
        v8::TryCatch tryCatch;
        result = v8::Persistent<v8::Value>::New(script->Run());
        v8::Handle<v8::Value> exception = tryCatch.Exception();
        if (!exception.IsEmpty())
        {
            v8::String::AsciiValue msg(exception);
            throw MakeStringException(MSGAUD_user, 0, "v8embed: %s", *msg);
        }
    }

protected:
    v8::Isolate *isolate;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Script> script;
    v8::Persistent<v8::Value> result;
};

static __thread V8JavascriptEmbedFunctionContext * theFunctionContext;  // We reuse per thread, for speed
static __thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    if (theFunctionContext)
    {
        ::Release(theFunctionContext);
        theFunctionContext = NULL;
    }
    if (threadHookChain)
    {
        (*threadHookChain)();
        threadHookChain = NULL;
    }
}

class V8JavascriptEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    V8JavascriptEmbedContext()
    {
    }
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options)
    {
        return createFunctionContextEx(NULL, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, unsigned flags, const char *options)
    {
        if (flags & EFimport)
            UNSUPPORTED("IMPORT");
        if (!theFunctionContext)
        {
            theFunctionContext = new V8JavascriptEmbedFunctionContext;
            threadHookChain = addThreadTermFunc(releaseContext);
        }
        return LINK(theFunctionContext);
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options)
    {
        throwUnexpected();
    }
} theEmbedContext;


extern IEmbedContext* getEmbedContext()
{
    return LINK(&theEmbedContext);
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
