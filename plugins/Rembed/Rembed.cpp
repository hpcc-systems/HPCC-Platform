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

#ifdef RCPP_HEADER_ONLY
// NOTE - these symbols need to be hidden from being exported from the Rembed .so file as RInside tries to dynamically
// load them from Rcpp.so

// If future versions of Rcpp add any (in Rcpp/routines.h) they may need to be added here too.

#define type2name HIDE_RCPP_type2name
#define enterRNGScope HIDE_RCPP_enterRNGScope
#define exitRNGScope HIDE_RCPP_exitRNGScope
#define get_string_buffer HIDE_RCPP_get_string_buffer
#define get_Rcpp_namespace HIDE_RCPP_get_Rcpp_namespace
#define mktime00 HIDE_RCPP_mktime00_
#define gmtime_ HIDE_RCPP_gmtime_

#define rcpp_get_stack_trace HIDE_RCPP_rcpp_get_stack_trace
#define rcpp_set_stack_trace HIDE_RCPP_rcpp_set_stack_trace
#define demangle HIDE_RCPP_demangle
#define short_file_name HIDE_RCPP_short_file_name
#define stack_trace HIDE_RCPP_stack_trace
#define get_string_elt HIDE_RCPP_get_string_elt
#define char_get_string_elt HIDE_RCPP_char_get_string_elt
#define set_string_elt HIDE_RCPP_set_string_elt
#define char_set_string_elt HIDE_RCPP_char_set_string_elt
#define get_string_ptr HIDE_RCPP_get_string_ptr
#define get_vector_elt HIDE_RCPP_get_vector_elt
#define set_vector_elt HIDE_RCPP_set_vector_elt
#define get_vector_ptr HIDE_RCPP_get_vector_ptr
#define char_nocheck HIDE_RCPP_char_nocheck
#define dataptr HIDE_RCPP_dataptr
#define getCurrentScope HIDE_RCPP_getCurrentScope
#define setCurrentScope HIDE_RCPP_setCurrentScope
#define get_cache HIDE_RCPP_get_cache
#define reset_current_error HIDE_RCPP_reset_current_error
#define error_occured HIDE_RCPP_error_occured
#define rcpp_get_current_error HIDE_RCPP_rcpp_get_current_error
#endif

#include "RInside.h"

#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield_imp.hpp"
#include "nbcd.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] =
{ "R Embed Helper 1.0.0", NULL };

static const char *version = "R Embed Helper 1.0.0";

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
    pb->moduleName = "+R+"; // Hack - we don't want to export any ECL, but if we don't export something,
    pb->ECL = "";           // Hack - the dll is unloaded at startup when compiling, and the R runtime closes stdin when unloaded
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "R Embed Helper";
    return true;
}

#ifdef _WIN32
    EXTERN_C IMAGE_DOS_HEADER __ImageBase;
#endif

#define UNSUPPORTED(feature) throw MakeStringException(MSGAUD_user, 0, "Rembed: UNSUPPORTED feature: %s", feature)
#define FAIL(msg) throw MakeStringException(MSGAUD_user, 0, "Rembed: Rcpp error: %s", msg)

namespace Rembed
{

class OwnedRoxieRowSet : public ConstPointerArray
{
public:
    ~OwnedRoxieRowSet()
    {
        ForEachItemIn(idx, *this)
            rtlReleaseRow(item(idx));
    }
};

// Use a global object to ensure that the R instance is initialized only once
// Because of R's dodgy stack checks, we also have to do so on main thread

static class RGlobalState
{
public:
    RGlobalState()
    {
        const char *args[] = {"R", "--slave" };
        R = new RInside(2, args, true, false, true);  // Setting interactive mode=true prevents R syntax errors from terminating the process
        // The R code for checking stack limits assumes that all calls are on the same thread
        // as the original context was created on - this will not always be true in ECL (and hardly
        // ever true in Roxie
        // Setting the stack limit to -1 disables this check
        R_CStackLimit = -1;
// Make sure we are never unloaded (as R does not support it)
// we do this by doing a dynamic load of the Rembed library
#ifdef _WIN32
        char path[_MAX_PATH];
        ::GetModuleFileName((HINSTANCE)&__ImageBase, path, _MAX_PATH);
        if (strstr(path, "Rembed"))
        {
            HINSTANCE h = LoadSharedObject(path, false, false);
            DBGLOG("LoadSharedObject returned %p", h);
        }
#else
        FILE *diskfp = fopen("/proc/self/maps", "r");
        if (diskfp)
        {
            char ln[_MAX_PATH];
            while (fgets(ln, sizeof(ln), diskfp))
            {
                if (strstr(ln, "libRembed"))
                {
                    const char *fullName = strchr(ln, '/');
                    if (fullName)
                    {
                        char *tail = (char *) strstr(fullName, SharedObjectExtension);
                        if (tail)
                        {
                            tail[strlen(SharedObjectExtension)] = 0;
                            HINSTANCE h = LoadSharedObject(fullName, false, false);
                            break;
                        }
                    }
                }
            }
            fclose(diskfp);
        }
#endif
    }
    ~RGlobalState()
    {
        delete R;
    }
    RInside *R;
}* globalState = NULL;

static CriticalSection RCrit;  // R is single threaded - need to own this before making any call to R

static RGlobalState *queryGlobalState()
{
    CriticalBlock b(RCrit);
    if (!globalState)
        globalState = new RGlobalState;
    return globalState;
}

extern void unload()
{
    CriticalBlock b(RCrit);
    if (globalState)
        delete globalState;
    globalState = NULL;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    queryGlobalState(); // make sure gets loaded by main thread
    return true;
}
MODULE_EXIT()
{
// Don't unload, because R seems to have problems with being reloaded, i.e. crashes on next use
//    unload();
}

// A RDataFrameHeaderBuilder object is used to construct the header for an R dataFrame from an ECL row

class RDataFrameHeaderBuilder : public CInterfaceOf<IFieldProcessor>
{
public:
    RDataFrameHeaderBuilder()
    {
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        addField(field);
    }
    virtual void processSetAll(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned elements, bool isAll, const byte *data)
    {
        UNSUPPORTED("SET fields");
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned rows)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
    Rcpp::CharacterVector namevec;
protected:
    void addField(const RtlFieldInfo * field)
    {
        namevec.push_back(field->name->queryStr());
    }
};

// A RDataFrameHeaderBuilder object is used to construct the header for an R dataFrame from an ECL row

class RDataFrameAppender : public CInterfaceOf<IFieldProcessor>
{
public:
    RDataFrameAppender(Rcpp::List &_list) : list(_list)
    {
        colIdx = 0;
        rowIdx = 0;
    }
    inline void setRowIdx(unsigned _idx)
    {
        colIdx = 0;
        rowIdx = _idx;
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        std::string s(value, len);
        Rcpp::List column = list[colIdx];
        column[rowIdx] = s;
        colIdx++;
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        Rcpp::List column = list[colIdx];
        column[rowIdx] = value;
        colIdx++;
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        std::vector<byte> vval;
        const byte *cval = (const byte *) value;
        vval.assign(cval, cval+len);
        Rcpp::List column = list[colIdx];
        column[rowIdx] = vval;
        colIdx++;
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        Rcpp::List column = list[colIdx];
        column[rowIdx] = (long int) value;  // Rcpp does not support int64
        colIdx++;
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        Rcpp::List column = list[colIdx];
        column[rowIdx] = (unsigned long int) value; // Rcpp does not support int64
        colIdx++;
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        Rcpp::List column = list[colIdx];
        column[rowIdx] = value;
        colIdx++;
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setDecimal(digits, precision, value);
        Rcpp::List column = list[colIdx];
        column[rowIdx] = val.getReal();
        colIdx++;
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setUDecimal(digits, precision, value);
        Rcpp::List column = list[colIdx];
        column[rowIdx] = val.getReal();
        colIdx++;
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processSetAll(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned elements, bool isAll, const byte *data)
    {
        UNSUPPORTED("SET fields");
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned rows)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    unsigned rowIdx;
    unsigned colIdx;
    Rcpp::List &list;
};

// A RRowBuilder object is used to construct an ECL row from a R dataframe and row index

class RRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    RRowBuilder(Rcpp::DataFrame &_frame)
    : frame(_frame)
    {
        rowIdx = 0;
        colIdx = 0;
    }
    inline void setRowIdx(unsigned _rowIdx)
    {
        rowIdx = _rowIdx;
        colIdx = 0;
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return ::Rcpp::as<bool>(elem);
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &__len, void * &__result)
    {
        nextField(field);
        std::vector<byte> vval = ::Rcpp::as<std::vector<byte> >(elem);
        rtlStrToDataX(__len, __result, vval.size(), vval.data());
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return ::Rcpp::as<double>(elem);
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return ::Rcpp::as<long int>(elem); // Should really be long long, but RInside does not support that
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return ::Rcpp::as<unsigned long int>(elem); // Should really be long long, but RInside does not support that
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &__len, char * &__result)
    {
        nextField(field);
        std::string str = ::Rcpp::as<std::string>(elem);
        rtlStrToStrX(__len, __result, str.length(), str.data());
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        nextField(field);
        double ret = ::Rcpp::as<double>(elem);
        value.setReal(ret);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        UNSUPPORTED("SET fields");
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        UNSUPPORTED("SET fields");
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    void nextField(const RtlFieldInfo * field)
    {
        // NOTE - we could put support for looking up columns by name here, but for efficiency reasons we only support matching by position
        Rcpp::RObject colObject = frame[colIdx];
        Rcpp::List column = ::Rcpp::as<Rcpp::List>(colObject); // MORE - this can crash if wrong type came from R. But I can't work out how to test that
        Rcpp::RObject t = column[rowIdx];
        elem = t;
        colIdx++;
    }
    Rcpp::DataFrame frame;
    unsigned rowIdx;
    unsigned colIdx;
    Rcpp::RObject elem;
};

static size32_t getRowResult(RInside::Proxy &result, ARowBuilder &builder)
{
     // To return a single row, we expect a dataframe (with 1 row)...
     Rcpp::DataFrame dFrame = ::Rcpp::as<Rcpp::DataFrame>(result);   // Note that this will also accept (and convert) a list
     RRowBuilder myRRowBuilder(dFrame);
     const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
     assertex(typeInfo);
     RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
     return typeInfo->build(builder, 0, &dummyField, myRRowBuilder);
}

// A R function that returns a dataset will return a RRowStream object that can be
// interrogated to return each row of the result in turn

class RRowStream : public CInterfaceOf<IRowStream>
{
public:
    RRowStream(RInside::Proxy &_result, IEngineRowAllocator *_resultAllocator)
      : dFrame(::Rcpp::as<Rcpp::DataFrame>(_result)),
        myRRowBuilder(dFrame)
    {
        resultAllocator.set(_resultAllocator);
        // A DataFrame is a list of columns
        // Each column is a vector (and all columns should be the same length)
        unsigned numColumns = dFrame.length();
        assertex(numColumns > 0);
        Rcpp::List col1 = dFrame[0];
        numRows = col1.length();
        idx = 0;
    }
    virtual const void *nextRow()
    {
        CriticalBlock b(RCrit);
        if (!resultAllocator)
            return NULL;
        if (idx >= numRows)
        {
            stop();
            return NULL;
        }
        RtlDynamicRowBuilder builder(resultAllocator);
        const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        myRRowBuilder.setRowIdx(idx);
        try
        {
            size32_t len = typeInfo->build(builder, 0, &dummyField, myRRowBuilder);
            idx++;
            return builder.finalizeRowClear(len);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual void stop()
    {
        resultAllocator.clear();
    }

protected:
    Rcpp::DataFrame dFrame;
    Linked<IEngineRowAllocator> resultAllocator;
    RRowBuilder myRRowBuilder;
    unsigned numRows;
    unsigned idx;
};


// Each call to a R function will use a new REmbedFunctionContext object
// This takes care of ensuring that the critsec is locked while we are executing R code,
// and released when we are not

class REmbedFunctionContext: public CInterfaceOf<IEmbedFunctionContext>
{
public:
    REmbedFunctionContext(RInside &_R, const char *options)
    : R(_R), block(RCrit), result(R_NilValue)
    {
    }
    ~REmbedFunctionContext()
    {
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


    virtual bool getBooleanResult()
    {
        try
        {
            return ::Rcpp::as<bool>(result);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual void getDataResult(size32_t &__len, void * &__result)
    {
        try
        {
            std::vector<byte> vval = ::Rcpp::as<std::vector<byte> >(result);
            rtlStrToDataX(__len, __result, vval.size(), vval.data());
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual double getRealResult()
    {
        try
        {
            return ::Rcpp::as<double>(result);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual __int64 getSignedResult()
    {
        try
        {
            return ::Rcpp::as<long int>(result); // Should really be long long, but RInside does not support that
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        try
        {
            return ::Rcpp::as<unsigned long int>(result); // Should really be long long, but RInside does not support that
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        try
        {
            std::string str = ::Rcpp::as<std::string>(result);
            rtlStrToStrX(__len, __result, str.length(), str.data());
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        UNSUPPORTED("Unicode/UTF8 results");
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        UNSUPPORTED("Unicode/UTF8 results");
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int _elemType, size32_t elemSize)
    {
        try
        {
            type_t elemType = (type_t) _elemType;
            __isAllResult = false;
            switch(elemType)
            {

#define FETCH_ARRAY(type) \
{  \
    std::vector<type> vval = ::Rcpp::as< std::vector<type> >(result); \
    rtlStrToDataX(__resultBytes, __result, vval.size()*elemSize, (const void *) vval.data()); \
}

            case type_boolean:
            {
                std::vector<bool> vval = ::Rcpp::as< std::vector<bool> >(result);
                size32_t size = vval.size();
                // Vector of bool is odd, and can't be retrieved via data()
                // Instead we need to iterate, I guess
                rtlDataAttr out(size);
                bool *outData = (bool *) out.getdata();
                for (std::vector<bool>::iterator iter = vval.begin(); iter < vval.end(); iter++)
                {
                    *outData++ = *iter;
                }
                __resultBytes = size;
                __result = out.detachdata();
                break;
            }
            case type_int:
                /* if (elemSize == sizeof(signed char))  // rcpp does not seem to support...
                    FETCH_ARRAY(signed char)
                else */ if (elemSize == sizeof(short))
                    FETCH_ARRAY(short)
                else if (elemSize == sizeof(int))
                    FETCH_ARRAY(int)
                else if (elemSize == sizeof(long))    // __int64 / long long does not work...
                    FETCH_ARRAY(long)
                else
                    rtlFail(0, "Rembed: Unsupported result type");
                break;
            case type_unsigned:
                if (elemSize == sizeof(byte))
                    FETCH_ARRAY(byte)
                else if (elemSize == sizeof(unsigned short))
                    FETCH_ARRAY(unsigned short)
                else if (elemSize == sizeof(unsigned int))
                    FETCH_ARRAY(unsigned int)
                else if (elemSize == sizeof(unsigned long))    // __int64 / long long does not work...
                    FETCH_ARRAY(unsigned long)
                else
                    rtlFail(0, "Rembed: Unsupported result type");
                break;
            case type_real:
                if (elemSize == sizeof(float))
                    FETCH_ARRAY(float)
                else if (elemSize == sizeof(double))
                    FETCH_ARRAY(double)
                else
                    rtlFail(0, "Rembed: Unsupported result type");
                break;
            case type_string:
            case type_varstring:
            {
                std::vector<std::string> vval = ::Rcpp::as< std::vector<std::string> >(result);
                size32_t numResults = vval.size();
                rtlRowBuilder out;
                byte *outData = NULL;
                size32_t outBytes = 0;
                if (elemSize != UNKNOWN_LENGTH)
                {
                    outBytes = numResults * elemSize;  // MORE - check for overflow?
                    out.ensureAvailable(outBytes);
                    outData = out.getbytes();
                }
                for (std::vector<std::string>::iterator iter = vval.begin(); iter < vval.end(); iter++)
                {
                    size32_t lenBytes = (*iter).size();
                    const char *text = (*iter).data();
                    if (elemType == type_string)
                    {
                        if (elemSize == UNKNOWN_LENGTH)
                        {
                            out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                            outData = out.getbytes() + outBytes;
                            * (size32_t *) outData = lenBytes;
                            rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                            outBytes += lenBytes + sizeof(size32_t);
                        }
                        else
                        {
                            rtlStrToStr(elemSize, outData, lenBytes, text);
                            outData += elemSize;
                        }
                    }
                    else
                    {
                        if (elemSize == UNKNOWN_LENGTH)
                        {
                            out.ensureAvailable(outBytes + lenBytes + 1);
                            outData = out.getbytes() + outBytes;
                            rtlStrToVStr(0, outData, lenBytes, text);
                            outBytes += lenBytes + 1;
                        }
                        else
                        {
                            rtlStrToVStr(elemSize, outData, lenBytes, text);  // Fixed size null terminated strings... weird.
                            outData += elemSize;
                        }
                    }
                }
                __resultBytes = outBytes;
                __result = out.detachdata();
                break;
            }
            default:
                rtlFail(0, "REmbed: Unsupported result type");
                break;
            }
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }

    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        try
        {
            return new RRowStream(result, _resultAllocator);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        try
        {
            RtlDynamicRowBuilder rowBuilder(_resultAllocator);
            size32_t len = Rembed::getRowResult(result, rowBuilder);
            return (byte *) rowBuilder.finalizeRowClear(len);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        try
        {
            return Rembed::getRowResult(result, builder);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        R[name] = val;
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        std::vector<byte> vval;
        const byte *cval = (const byte *) val;
        vval.assign(cval, cval+len);
        R[name] = vval;
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        R[name] = val;
    }
    virtual void bindRealParam(const char *name, double val)
    {
        R[name] = val;
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        R[name] = (long int) val;
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        R[name] = (long int) val;
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        R[name] = (long int) val;
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        R[name] = (unsigned long int) val;
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        std::string s(val, len);
        R[name] = s;
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        R[name] = val;
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        rtlFail(0, "Rembed: Unsupported parameter type UTF8");
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        rtlFail(0, "Rembed: Unsupported parameter type UNICODE");
    }

    virtual void bindSetParam(const char *name, int _elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        if (isAll)
            rtlFail(0, "Rembed: Unsupported parameter type ALL");
        type_t elemType = (type_t) _elemType;
        int numElems = totalBytes / elemSize;
        switch(elemType)
        {

#define BIND_ARRAY(type) \
{  \
    std::vector<type> vval; \
    const type *start = (const type *) setData; \
    vval.assign(start, start+numElems); \
    R[name] = vval; \
}

        case type_boolean:
            BIND_ARRAY(bool)
            break;
        case type_int:
            /* if (elemSize == sizeof(signed char))  // No binding exists in rcpp
                BIND_ARRAY(signed char)
            else */ if (elemSize == sizeof(short))
                BIND_ARRAY(short)
            else if (elemSize == sizeof(int))
                BIND_ARRAY(int)
            else if (elemSize == sizeof(long))    // __int64 / long long does not work...
                BIND_ARRAY(long)
            else
                rtlFail(0, "Rembed: Unsupported parameter type");
            break;
        case type_unsigned:
            if (elemSize == sizeof(unsigned char))
                BIND_ARRAY(unsigned char)
            else if (elemSize == sizeof(unsigned short))
                BIND_ARRAY(unsigned short)
            else if (elemSize == sizeof(unsigned int))
                BIND_ARRAY(unsigned int)
            else if (elemSize == sizeof(unsigned long))    // __int64 / long long does not work...
                BIND_ARRAY(unsigned long)
            else
                rtlFail(0, "Rembed: Unsupported parameter type");
            break;
        case type_real:
            if (elemSize == sizeof(float))
                BIND_ARRAY(float)
            else if (elemSize == sizeof(double))
                BIND_ARRAY(double)
            else
                rtlFail(0, "Rembed: Unsupported parameter type");
            break;
        case type_string:
        case type_varstring:
        {
            std::vector<std::string> vval;
            const byte *inData = (const byte *) setData;
            const byte *endData = inData + totalBytes;
            while (inData < endData)
            {
                int thisSize;
                if (elemSize == UNKNOWN_LENGTH)
                {
                    if (elemType==type_varstring)
                        thisSize = strlen((const char *) inData) + 1;
                    else
                    {
                        thisSize = * (size32_t *) inData;
                        inData += sizeof(size32_t);
                    }
                }
                else
                    thisSize = elemSize;
                std::string s((const char *) inData, thisSize);
                vval.push_back(s);
                inData += thisSize;
                numElems++;
            }
            R[name] = vval;
            break;
        }
        default:
            rtlFail(0, "REmbed: Unsupported parameter type");
            break;
        }
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *row)
    {
        // We create a single-row dataframe
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);

        RDataFrameHeaderBuilder headerBuilder;
        typeInfo->process(row, row, &dummyField, headerBuilder); // Sets up the R dataframe from the first ECL row
        Rcpp::List myList(headerBuilder.namevec.length());
        myList.attr("names") = headerBuilder.namevec;
        for (int i=0; i<myList.length(); i++)
        {
            Rcpp::List column(1);
            myList[i] = column;
        }
        RDataFrameAppender frameBuilder(myList);
        Rcpp::StringVector row_names(1);
        frameBuilder.setRowIdx(0);
        typeInfo->process(row, row, &dummyField, frameBuilder);
        row_names(0) = "1";
        myList.attr("class") = "data.frame";
        myList.attr("row.names") = row_names;
        R[name] = myList;
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);

        OwnedRoxieRowSet rows;
        loop
        {
            const byte *row = (const byte *) val->ungroupedNextRow();
            if (!row)
                break;
            rows.append(row);
        }
        const byte *firstrow = (const byte *) rows.item(0);

        RDataFrameHeaderBuilder headerBuilder;
        typeInfo->process(firstrow, firstrow, &dummyField, headerBuilder); // Sets up the R dataframe from the first ECL row
        Rcpp::List myList(headerBuilder.namevec.length());
        myList.attr("names") = headerBuilder.namevec;
        for (int i=0; i<myList.length(); i++)
        {
            Rcpp::List column(rows.length());
            myList[i] = column;
        }
        RDataFrameAppender frameBuilder(myList);
        Rcpp::StringVector row_names(rows.length());
        ForEachItemIn(idx, rows)
        {
            const byte * row = (const byte *) rows.item(idx);
            frameBuilder.setRowIdx(idx);
            typeInfo->process(row, row, &dummyField, frameBuilder);
            StringBuffer rowname;
            rowname.append(idx+1);
            row_names(idx) = rowname.str();
        }
        myList.attr("class") = "data.frame";
        myList.attr("row.names") = row_names;
        R[name] = myList;
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        StringBuffer text(rtlUtf8Size(lenChars, utf), utf);
        text.stripChar('\r');
        func.assign(text.str());
    }

    virtual void callFunction()
    {
        try
        {
            result = R.parseEval(func);
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
private:
    RInside &R;
    RInside::Proxy result;
    std::string func;
    CriticalBlock block;
};

class REmbedContext: public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options)
    {
        return createFunctionContextEx(NULL, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, unsigned flags, const char *options)
    {
        return new REmbedFunctionContext(*queryGlobalState()->R, options);
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options)
    {
        throwUnexpected();
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new REmbedContext;
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
