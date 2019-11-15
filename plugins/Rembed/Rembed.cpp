/*##############################################################################

 HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

 Licensed under the GPL, Version 2.0 or later
 you may not use this file except in compliance with the License.

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
#include "Rinterface.h"

#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield.hpp"
#include "nbcd.hpp"
#include "enginecontext.hpp"

static const char * compatibleVersions[] =
{ "R Embed Helper 1.0.0", NULL };

static const char *version = "R Embed Helper 1.0.0";

extern "C" DECL_EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
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

using Rcpp::_;

namespace Rembed
{
// Copied from Rcpp 0.12.15's meat/Environment.h, in case an older version of Rcpp is in use
inline Rcpp::Environment _new_env(SEXP parent, int size = 29) {
    Rcpp::Function newEnv("new.env", R_BaseNamespace);
    return newEnv(_["size"] = size, _["parent"] = parent);
}


__declspec(noreturn) static void failx(const char *msg, ...) __attribute__((format(printf, 1, 2), noreturn));

static void failx(const char *message, ...)
{
    va_list args;
    va_start(args,message);
    StringBuffer msg;
    msg.append("rembed: ").valist_appendf(message,args);
    va_end(args);
    rtlFail(0, msg.str());
}

class OwnedRoxieRowSet : public ConstPointerArray
{
public:
    ~OwnedRoxieRowSet()
    {
        ForEachItemIn(idx, *this)
            rtlReleaseRow(item(idx));
    }
};

class REnvironment : public CInterfaceOf<IInterface>
{
public:
    inline REnvironment(Rcpp::Environment _env)
    : env(_env)
    {
    }
    inline Rcpp::Environment &query()
    {
        return env;
    }
private:
    REnvironment(const REnvironment &);
    Rcpp::Environment env;
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
        StringBuffer modname;
        if (findLoadedModule(modname, "Rembed"))
        {
            HINSTANCE h = LoadSharedObject(modname, false, false);
            // Deliberately leak this handle
        }
#endif
    }
    ~RGlobalState()
    {
        delete R;
    }
    REnvironment *getNamedScope(const char *key, bool &isNew)
    {
        Linked<REnvironment> ret = preservedScopes.getValue(key);
        if (!ret)
        {
            ret.setown(new REnvironment(_new_env(Rcpp::Environment::global_env())));
            preservedScopes.setValue(key, ret);  // NOTE - links arg
            isNew = true;
        }
        else
            isNew = false;
        return ret.getClear();
    }
    void releaseNamedScope(const char *key)
    {
        preservedScopes.remove(key);
    }
    static void unregister(const char *key);
    RInside *R;
private:
    MapStringToMyClass<REnvironment> preservedScopes;
}* globalState = NULL;

static CriticalSection RCrit;  // R is single threaded - need to own this before making any call to R

void RGlobalState::unregister(const char *key)
{
    CriticalBlock b(RCrit);
    if (globalState)
        globalState->releaseNamedScope(key);
}

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

static void getFieldNames(Rcpp::CharacterVector &namevec, const RtlTypeInfo *typeInfo)
{
    const RtlFieldInfo * const *fields = typeInfo->queryFields();
    while (*fields)
    {
        const RtlFieldInfo *child = *fields;
        // MORE - nested records may make this interesting
        namevec.push_back(child->name);
        fields++;
    }
}

/*
 * Create a blank dataframe of the specified size, ready to fill with data from an ECL dataset
 */
static Rcpp::DataFrame createDataFrame(const RtlTypeInfo *typeInfo, unsigned numRows)
{
    Rcpp::CharacterVector namevec;
    getFieldNames(namevec, typeInfo);
    Rcpp::List frame(namevec.length()); // Number of columns
    frame.attr("names") = namevec;
    const RtlFieldInfo * const *fields = typeInfo->queryFields();
    for (int i=0; i< frame.length(); i++)
    {
        assertex(*fields);
        const RtlFieldInfo *child = *fields;
        switch (child->type->getType())
        {
        case type_boolean:
        {
            Rcpp::LogicalVector column(numRows);
            frame[i] = column;
            break;
        }
        case type_int:
        {
            Rcpp::IntegerVector column(numRows);
            frame[i] = column;
            break;
        }
        case type_real:
        case type_decimal:
        {
            Rcpp::NumericVector column(numRows);
            frame[i] = column;
            break;
        }
        case type_string:
        case type_varstring:
        {
            Rcpp::StringVector column(numRows);
            frame[i] = column;
            break;
        }
        default:
        {
            Rcpp::List column(numRows);
            frame[i] = column;
            break;
        }
        }
        fields++;
    }
    Rcpp::StringVector row_names(numRows);
    for (unsigned row = 0; row < numRows; row++)
    {
        StringBuffer rowname;
        rowname.append(row+1);
        row_names(row) = rowname.str();
    }
    frame.attr("class") = "data.frame";
    frame.attr("row.names") = row_names;
    return frame;
}

/*
 * Create a blank list of the specified type, ready to fill with data from an ECL record
 */
static Rcpp::List createList(const RtlTypeInfo *typeInfo)
{
    Rcpp::CharacterVector namevec;
    getFieldNames(namevec, typeInfo);
    Rcpp::List childRec(namevec.length());
    childRec.attr("names") = namevec;
    return childRec;
}

// A RDataFrameAppender object is used append a row to a R dataFrame from an ECL row

class RDataFrameAppender : public CInterfaceOf<IFieldProcessor>
{
public:
    RDataFrameAppender(Rcpp::DataFrame &_frame)
    {
        stack.append(*new DataFramePosition(_frame));
    }
    RDataFrameAppender(Rcpp::List &_list)
    {
        stack.append(*new ListPosition(_list, nullptr));
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field) override
    {
        std::string s(value, len);
        if (inSet)
            theStringSet[setIndex++] = s;
        else
            stack.tos().setString(s);
    }
    virtual void processBool(bool value, const RtlFieldInfo * field) override
    {
        if (inSet)
            theBoolSet[setIndex++] = value;
        else
            stack.tos().setBool(value);
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field) override
    {
        std::vector<byte> vval;
        const byte *cval = (const byte *) value;
        vval.assign(cval, cval+len);
        unsigned r;
        Rcpp::List l = stack.tos().cell(r);
        l[r] = vval;
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field) override
    {
        if (inSet)
            theIntSet[setIndex++] = (long int) value;
        else
            stack.tos().setInt(value);
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field) override
    {
        if (inSet)
            theIntSet[setIndex++] = (unsigned long int) value;
        else
            stack.tos().setUInt(value);
    }
    virtual void processReal(double value, const RtlFieldInfo * field) override
    {
        if (inSet)
            theRealSet[setIndex++] = value;
        else
            stack.tos().setReal(value);
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) override
    {
        Decimal val;
        val.setDecimal(digits, precision, value);
        if (inSet)
            theRealSet[setIndex++] = val.getReal();
        else
            stack.tos().setReal(val.getReal());
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) override
    {
        Decimal val;
        val.setUDecimal(digits, precision, value);
        if (inSet)
            theRealSet[setIndex++] = val.getReal();
        else
            stack.tos().setReal(val.getReal());
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field) override
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field) override
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field) override
    {
        UNSUPPORTED("Unicode/UTF8 fields");
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned elements, bool isAll, const byte *data) override
    {
        if (isAll)
            UNSUPPORTED("ALL sets are not supported");
        unsigned r;
        Rcpp::List l = stack.tos().cell(r);
        switch (field->type->queryChildType()->fieldType & RFTMkind)
        {
        case type_boolean:
            theBoolSet = Rcpp::LogicalVector(elements);
            l[r] = theBoolSet;
            break;
        case type_int:
            theIntSet = Rcpp::IntegerVector(elements);
            l[r] = theIntSet;
            break;
        case type_decimal:
        case type_real:
            theRealSet = Rcpp::NumericVector(elements);
            l[r] = theRealSet;
            break;
        case type_string:
        case type_varstring:
            theStringSet = Rcpp::StringVector(elements);
            l[r] = theStringSet;
            break;
        default:
            UNSUPPORTED("SET types other than BOOLEAN, STRING, INTEGER and REAL");
        }
        setIndex = 0;
        inSet = true;
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned rows) override
    {
        Rcpp::DataFrame myFrame = createDataFrame(field->type->queryChildType(), rows);
        unsigned r;
        Rcpp::List l = stack.tos().cell(r);
        l[r] = myFrame;
        push(myFrame);
        firstField = true;
        return true;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field) override
    {
        // We see this at the start of each row in a child dataset, but also at the start of a nested record
        // If the field is the outer field, ignore...
        if (firstField)
            firstField = false;
        else
        {
            Rcpp::List childRec = createList(field->type);
            unsigned r;
            Rcpp::List l = stack.tos().cell(r);
            l[r] = childRec;
            stack.append(*new ListPosition(childRec, field));
        }
        stack.tos().nextRow();
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field) override
    {
        inSet = false;
    }
    virtual void processEndDataset(const RtlFieldInfo * field) override
    {
        pop();
    }
    virtual void processEndRow(const RtlFieldInfo * field) override
    {
        if (stack.tos().isNestedRow(field))
            pop();
        else
            firstField = true;
    }
protected:
    interface IDataListPosition : public IInterface
    {
        virtual Rcpp::List cell(unsigned &row) = 0;
        virtual void setBool(bool value) = 0;
        virtual void setInt(__int64 value) = 0;
        virtual void setUInt(unsigned __int64 value) = 0;
        virtual void setReal(double value) = 0;
        virtual void setString(const std::string &s) = 0;
        virtual void nextRow() = 0;
        virtual bool isNestedRow(const RtlFieldInfo *_field) const = 0;
    };
    class DataFramePosition : public CInterfaceOf<IDataListPosition>
    {
    public:
        DataFramePosition(Rcpp::DataFrame _frame) : frame(_frame) {}
        virtual Rcpp::List cell(unsigned &row) override
        {
            row = rowIdx-1;        // nextRow is called before the first row, so rowIdx is 1-based
            curCell = frame[colIdx++];
            return curCell;
        }
        virtual void setBool(bool value) override
        {
            unsigned row = rowIdx-1;        // nextRow is called before the first row, so rowIdx is 1-based
            Rcpp::LogicalVector l = frame[colIdx++];
            l[row] = value;
        }
        virtual void setInt(__int64 value) override
        {
            unsigned row = rowIdx-1;
            Rcpp::IntegerVector l = frame[colIdx++];
            l[row] = (long int) value;  // Rcpp does not support int64
        }
        virtual void setUInt(unsigned __int64 value) override
        {
            unsigned row = rowIdx-1;
            Rcpp::IntegerVector l = frame[colIdx++];
            l[row] = (unsigned long int) value;  // Rcpp does not support int64
        }
        virtual void setReal(double value) override
        {
            unsigned row = rowIdx-1;
            Rcpp::NumericVector l = frame[colIdx++];
            l[row] = value;
        }
        virtual void setString(const std::string &value) override
        {
            unsigned row = rowIdx-1;
            Rcpp::StringVector l = frame[colIdx++];
            l[row] = value;
        }
        virtual void nextRow() override
        {
            rowIdx++;
            colIdx = 0;
        }
        bool isNestedRow(const RtlFieldInfo *_field) const override
        {
            return false;
        }
    private:
        unsigned rowIdx = 0;
        unsigned colIdx = 0;
        Rcpp::DataFrame frame;
        Rcpp::List curCell;
    };
    class ListPosition : public CInterfaceOf<IDataListPosition>
    {
    public:
        ListPosition(Rcpp::List _list, const RtlFieldInfo *_field)
        : list(_list), field(_field)
        {}
        virtual Rcpp::List cell(unsigned &row) override
        {
            row = colIdx++;
            return list;
        }
        virtual void setBool(bool value) override
        {
            list[colIdx++] = value;
        }
        virtual void setInt(__int64 value) override
        {
            list[colIdx++] = (long int) value;  // Rcpp does not support int64
        }
        virtual void setUInt(unsigned __int64 value) override
        {
            list[colIdx++] = (unsigned long int) value;  // Rcpp does not support int64
        }
        virtual void setReal(double value) override
        {
            list[colIdx++] = value;
        }
        virtual void setString(const std::string &value) override
        {
            list[colIdx++] = value;
        }
        virtual void nextRow() override
        {
            colIdx = 0;
        }
        virtual bool isNestedRow(const RtlFieldInfo *_field) const override
        {
            return field==_field;
        }
    private:
        unsigned colIdx = 0;
        Rcpp::List list;
        const RtlFieldInfo *field;
    };
    void push(Rcpp::DataFrame frame)
    {
        stack.append(*new DataFramePosition(frame));
    }
    void pop()
    {
        stack.pop();
    }
    IArrayOf<IDataListPosition> stack;
    Rcpp::IntegerVector theIntSet;
    Rcpp::StringVector theStringSet;
    Rcpp::NumericVector theRealSet;
    Rcpp::LogicalVector theBoolSet;
    bool firstField = true;
    bool inSet = false;
    unsigned setIndex = 0;
};

// A RRowBuilder object is used to construct ECL rows from R dataframes or lists

class RRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    RRowBuilder(Rcpp::DataFrame &_frame, const RtlFieldInfo *_outerRow)
    : outerRow(_outerRow)
    {
        stack.append(*new RowState(_frame));
    }
    RRowBuilder(Rcpp::List &_list, const RtlFieldInfo *_outerRow)
    : outerRow(_outerRow)
    {
        stack.append(*new ListState(_list, nullptr));
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
        nextField(field);
        isAll = false;  // No concept of an 'all' set in R
        Rcpp::List childrec = ::Rcpp::as<Rcpp::List>(elem);  // MORE - is converting it to a list inefficient? Keeps the code simpler!
        stack.append(*new ListState(childrec, field));
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        return stack.tos().moreFields();
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        nextField(field);
        push();
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
        // We see this at the start of each row in a child dataset, but also at the start of a nested record
        // We want to ignore it if we are expecting the former case...
        if (firstField)
            firstField = false;
        else
        {
            nextField(field);
            Rcpp::List childrec = ::Rcpp::as<Rcpp::List>(elem);
            stack.append(*new ListState(childrec, field));
        }
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        firstField = true;
        IRowState &cur = stack.tos();
        return stack.tos().processNextRow();
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
        if (stack.tos().isNestedRow(field))
            pop();
    }
protected:
    interface IRowState : public IInterface
    {
        virtual Rcpp::RObject nextField() = 0;
        virtual bool processNextRow() = 0;
        virtual bool isNestedRow(const RtlFieldInfo *_field) const = 0;
        virtual bool moreFields() const = 0;
    };
    class RowState : public CInterfaceOf<IRowState>
    {
    public:
        RowState(Rcpp::DataFrame _frame) : frame(_frame)
        {
            /* these functions have been renamed in Rcpp 0.2.10, but the old names still work... */
            numRows = frame.nrows();
            numCols = frame.length();
        }
        bool moreFields() const override
        {
            return colIdx < numCols;
        }
        Rcpp::RObject nextField() override
        {
            assertex(colIdx < numCols && rowIdx-1 < numRows);
            Rcpp::RObject colObject = frame[colIdx];
            Rcpp::List column = ::Rcpp::as<Rcpp::List>(colObject); // MORE - this can crash if wrong type came from R. But I can't work out how to test that
            Rcpp::RObject elem = column[rowIdx-1];   // processNextRow gets called before first row, so it's 1-based
            colIdx++;
            return elem;
        }
        bool processNextRow() override
        {
            if (rowIdx < numRows)
            {
                rowIdx++;
                colIdx = 0;
                return true;
            }
            return false;
        }
        bool isNestedRow(const RtlFieldInfo *_field) const override
        {
            return false;
        }
    private:
        Rcpp::DataFrame frame;
        unsigned rowIdx = 0;
        unsigned colIdx = 0;
        unsigned numRows = 0;
        unsigned numCols = 0;
    };
    class ListState : public CInterfaceOf<IRowState>
    {
    public:
        ListState(Rcpp::List _list, const RtlFieldInfo *_field) : list(_list), field(_field)
        {
            numCols = list.length();
        }
        Rcpp::RObject nextField() override
        {
            assertex (colIdx < numCols);
            Rcpp::RObject elem = list[colIdx];
            colIdx++;
            return elem;
        }
        bool moreFields() const override
        {
            return colIdx < numCols;
        }
        bool processNextRow() override
        {
            throwUnexpected();
        }
        bool isNestedRow(const RtlFieldInfo *_field) const override
        {
            return field==_field;
        }
    private:
        Rcpp::List list;
        const RtlFieldInfo *field;
        unsigned colIdx = 0;
        unsigned numCols = 0;
    };
    void nextField(const RtlFieldInfo * field)
    {
        // NOTE - we could put support for looking up columns by name here, but for efficiency reasons we only support matching by position
        IRowState &cur = stack.tos();
        elem = cur.nextField();
    }
    void push()
    {
        stack.append(*new RowState(::Rcpp::as<Rcpp::DataFrame>(elem)));
    }
    void pop()
    {
        stack.pop();
    }
    IArrayOf<IRowState> stack;
    Rcpp::RObject elem;
    const RtlFieldInfo *outerRow;
    bool firstField = true;
};

static size32_t getRowResult(RInside::Proxy &result, ARowBuilder &builder)
{
     // To return a single row, we expect a list...
     Rcpp::List row = ::Rcpp::as<Rcpp::List>(result);
     const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
     assertex(typeInfo);
     RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
     RRowBuilder myRRowBuilder(row, &dummyField);
     return typeInfo->build(builder, 0, &dummyField, myRRowBuilder);
}

// A R function that returns a dataset will return a RRowStream object that can be
// interrogated to return each row of the result in turn

class RRowStream : public CInterfaceOf<IRowStream>
{
public:
    RRowStream(RInside::Proxy &_result, IEngineRowAllocator *_resultAllocator, const RtlTypeInfo *_typeInfo)
      : dFrame(::Rcpp::as<Rcpp::DataFrame>(_result)),
        resultAllocator(_resultAllocator),
        typeInfo(_typeInfo),
        dummyField("<row>", NULL, typeInfo),
        myRRowBuilder(dFrame, &dummyField)
    {
    }
    virtual const void *nextRow()
    {
        CriticalBlock b(RCrit);
        if (!resultAllocator)
            return NULL;
        try
        {
            if (!myRRowBuilder.processNextRow(&dummyField))
            {
                stop();
                return NULL;
            }
            RtlDynamicRowBuilder builder(resultAllocator);
            size32_t len = typeInfo->build(builder, 0, &dummyField, myRRowBuilder);
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
    const RtlTypeInfo *typeInfo;
    RtlFieldStrInfo dummyField;
    RRowBuilder myRRowBuilder;
};


// Each call to a R function will use a new REmbedFunctionContext object
// This takes care of ensuring that the critsec is locked while we are executing R code,
// and released when we are not

class REmbedFunctionContext: public CInterfaceOf<IEmbedFunctionContext>
{
public:
    REmbedFunctionContext(RInside &_R)
    : R(_R), block(RCrit), result(R_NilValue)
    {
    }
    void setScopes(ICodeContext *codeCtx, const char *_options)
    {
        StringArray options;
        options.appendList(_options, ",");
        StringBuffer scopeKey;
        const char *scopeKey2 = nullptr;
        bool registerCallback = false;
        bool wuidScope = false;
        IEngineContext *engine = nullptr;
        ForEachItemIn(idx, options)
        {
            const char *opt = options.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (strieq(optName, "globalscope"))
                    scopeKey2 = val;
                else if (strieq(optName, "persist"))
                {
                    if (scopeKey.length())
                        failx("persist option specified more than once");
                    if (strieq(val, "global"))
                        scopeKey.append("global");
                    else if (strieq(val, "workunit"))
                    {
                        engine = codeCtx->queryEngineContext();
                        wuidScope = true;
                        if (!engine)
                            failx("Persist mode 'workunit' not supported here");
                    }
                    else if (strieq(val, "query"))
                    {
                        engine = codeCtx->queryEngineContext();
                        wuidScope = false;
                        if (!engine)
                            failx("Persist mode 'query' not supported here");
                    }
                    else
                        failx("Unrecognized persist mode %s", val);
                }
                else
                    failx("Unrecognized option %s", optName.str());
            }
            else
                failx("Unrecognized option %s", opt);
        }
        if (engine)
            engine->getQueryId(scopeKey, wuidScope);
        if (scopeKey2)
            scopeKey.append(':').append(scopeKey2);
        if (scopeKey.length())
        {
            bool isNew;
            env.setown(globalState->getNamedScope(scopeKey, isNew));
            if (isNew && engine)
                engine->onTermination(RGlobalState::unregister, scopeKey.str(), wuidScope);
        }
        else
            env.setown(new REnvironment(_new_env(Rcpp::Environment::global_env())));
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
            return new RRowStream(result, _resultAllocator, _resultAllocator->queryOutputMeta()->queryTypeInfo());
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
        env->query()[name] = val;
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        std::vector<byte> vval;
        const byte *cval = (const byte *) val;
        vval.assign(cval, cval+len);
        env->query()[name] = vval;
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        env->query()[name] = val;
    }
    virtual void bindRealParam(const char *name, double val)
    {
        env->query()[name] = val;
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        env->query()[name] = (long int) val;
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        env->query()[name] = (long int) val;
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        env->query()[name] = (long int) val;
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        env->query()[name] = (unsigned long int) val;
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        std::string s(val, len);
        env->query()[name] = s;
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        env->query()[name] = val;
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        rtlFail(0, "Rembed: Unsupported parameter type UTF8");
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        rtlFail(0, "Rembed: Unsupported parameter type UNICODE");
    }

    virtual void bindSetParam(const char *name, int _elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
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
    env->query()[name] = vval; \
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
            env->query()[name] = vval;
            break;
        }
        default:
            rtlFail(0, "REmbed: Unsupported parameter type");
            break;
        }
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *row) override
    {
        // We create a list
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        Rcpp::List myList = createList(typeInfo);
        RDataFrameAppender frameBuilder(myList);
        typeInfo->process(row, row, &dummyField, frameBuilder);
        env->query()[name] = myList;
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        OwnedRoxieRowSet rows;
        for (;;)
        {
            const byte *row = (const byte *) val->ungroupedNextRow();
            if (!row)
                break;
            rows.append(row);
        }
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);

        Rcpp::DataFrame frame  = createDataFrame(typeInfo, rows.length());
        RDataFrameAppender frameBuilder(frame);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        ForEachItemIn(idx, rows)
        {
            const byte * row = (const byte *) rows.item(idx);
            typeInfo->process(row, row, &dummyField, frameBuilder);
        }
        env->query()[name] = frame;
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        StringBuffer text;
        text.append(rtlUtf8Size(lenChars, utf), utf);
        text.stripChar('\r');
        func.set(text.str());
    }
    virtual void loadCompiledScript(size32_t chars, const void *_script) override
    {
        throwUnexpected();
    }
    virtual void enter() override {}
    virtual void reenter(ICodeContext *codeCtx) override {}
    virtual void exit() override {}

    virtual void callFunction()
    {
        try
        {
            Rcpp::ExpressionVector exp(func) ;
            result = exp.eval(env->query());
        }
        catch (std::exception &E)
        {
            FAIL(E.what());
        }
    }
private:
    RInside &R;
    RInside::Proxy result;
    StringAttr func;
    CriticalBlock block;
    Owned<REnvironment> env;
};

class REmbedContext: public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
    {
        return createFunctionContextEx(nullptr, nullptr, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
    {
        Owned<REmbedFunctionContext> ret =  new REmbedFunctionContext(*queryGlobalState()->R);
        ret->setScopes(ctx, options);
        return ret.getClear();
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
    {
        throwUnexpected();
    }
};

extern DECL_EXPORT IEmbedContext* getEmbedContext()
{
    return new REmbedContext;
}

extern DECL_EXPORT void syntaxCheck(size32_t & __lenResult, char * & __result, const char *funcname, size32_t charsBody, const char * body, const char *argNames, const char *compilerOptions, const char *persistOptions)
{
    StringBuffer result;
    try
    {
        Owned<REmbedFunctionContext> ctx =  new REmbedFunctionContext(*queryGlobalState()->R);
        // MORE - could check supplied persistOptions are valid
        StringBuffer text;
        text.append(rtlUtf8Size(charsBody, body), body);
        text.stripChar('\r');
        Rcpp::ExpressionVector exp(text);
    }
    catch (std::exception &E)
    {
        result.append("Rembed: Parse error from R while checking embedded code"); // Unfortunately we don't get any info about the error position or nature, just "Parse error."
    }
    __lenResult = result.length();
    __result = result.detach();
}

} // namespace
