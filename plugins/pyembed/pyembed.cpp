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

#include "platform.h"
#include "Python.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
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

static const char * compatibleVersions[] = {
    "Python2.7 Embed Helper 1.0.0",
    NULL };

static const char *version = "Python2.7 Embed Helper 1.0.0";

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
    pb->moduleName = "python";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "Python2.7 Embed Helper";
    return true;
}

namespace pyembed {

// Use class OwnedPyObject for any objects that are not 'borrowed references'
// so that the appropriate Py_DECREF call is made when the OwnedPyObject goes
// out of scope, even if the function returns prematurely (such as via an exception).
// In particular, checkPythonError is a lot easier to call safely if this is used.

class OwnedPyObject
{
    PyObject *ptr;
public:
    inline OwnedPyObject() : ptr(NULL)     {}
    inline OwnedPyObject(PyObject *_ptr) : ptr(_ptr) {}
    inline ~OwnedPyObject()                { if (ptr) Py_DECREF(ptr); }
    inline PyObject * get() const           { return ptr; }
    inline PyObject * getClear()            { PyObject *ret = ptr; ptr = NULL; return ret; }
    inline PyObject * operator -> () const { return ptr; }
    inline operator PyObject *() const    { return ptr; }
    inline void clear()                     { if (ptr) Py_DECREF(ptr); ptr = NULL; }
    inline void setown(PyObject *_ptr)      { clear(); ptr = _ptr; }
    inline void set(PyObject *_ptr)         { clear(); ptr = _ptr; if (ptr) Py_INCREF(ptr);}
    inline PyObject *getLink()              { if (ptr) Py_INCREF(ptr); return ptr;}
    inline PyObject **ref()                 { return &ptr; }
};

// call checkPythonError to throw an exception if Python error state is set

static void checkPythonError()
{
    PyObject* err = PyErr_Occurred();
    if (err)
    {
        OwnedPyObject pType, pValue, pTraceBack;
        PyErr_Fetch(pType.ref(), pValue.ref(), pTraceBack.ref());
        OwnedPyObject valStr = PyObject_Str(pValue);
        PyErr_Clear();
        VStringBuffer errMessage("pyembed: %s", PyString_AsString(valStr));
        rtlFail(0, errMessage.str());
    }
}

// The Python Global Interpreter Lock (GIL) won't know about C++-created threads, so we need to
// call PyGILState_Ensure() and PyGILState_Release at the start and end of every function.
// Wrapping them in a class like this ensures that the release always happens even if
// the function exists prematurely

class GILstateWrapper
{
    PyGILState_STATE gstate;
public:
    GILstateWrapper()
    {
        gstate = PyGILState_Ensure();
    }
    ~GILstateWrapper()
    {
        PyGILState_Release(gstate);
    }
};

// There is a singleton PythonThreadContext per thread. This allows us to
// ensure that we can make repeated calls to a Python function efficiently.

class PythonThreadContext
{
public:
    PyThreadState *threadState;
public:
    PythonThreadContext()
    {
        threadState = PyEval_SaveThread();
    }
    ~PythonThreadContext()
    {
        PyEval_RestoreThread(threadState);
        script.clear();
    }

    inline PyObject * importFunction(size32_t lenChars, const char *utf)
    {
        size32_t bytes = rtlUtf8Size(lenChars, utf);
        StringBuffer text(bytes, utf);
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            // Name should be in the form module.function
            const char *funcname = strrchr(text, '.');
            if (!funcname)
                rtlFail(0, "pyembed: Expected module.function");
            StringBuffer modname(funcname-text, text);
            funcname++;  // skip the '.'
            // If the modname is preceded by a path, add it to the python path before importing
            const char *pathsep = strrchr(modname, PATHSEPCHAR);
            if (pathsep)
            {
                StringBuffer path(pathsep-modname, modname);
                modname.remove(0, 1+pathsep-modname);
                PyObject *sys_path = PySys_GetObject((char *) "path");
                OwnedPyObject new_path = PyString_FromString(path);
                if (sys_path)
                {
                    PyList_Insert(sys_path, 0, new_path);
                    checkPythonError();
                }
            }
            module.setown(PyImport_ImportModule(modname));
            checkPythonError();
            PyObject *dict = PyModule_GetDict(module);  // this is a borrowed reference and does not need to be released
            script.set(PyDict_GetItemString(dict, funcname));
            checkPythonError();
            if (!script || !PyCallable_Check(script))
                rtlFail(0, "pyembed: Object is not callable");
            prevtext.set(text);
        }
        return script.getLink();
    }

    inline PyObject *compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        size32_t bytes = rtlUtf8Size(lenChars, utf);
        StringBuffer text(bytes, utf);
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            // Try compiling as a eval first... if that fails, try as a script.
            text.stripChar('\r');
            script.setown(Py_CompileString(text, "", Py_eval_input));
            if (!script)
            {
                PyErr_Clear();
                StringBuffer wrapped;
                wrapPythonText(wrapped, text);
                script.setown(Py_CompileString(wrapped, "<embed>", Py_file_input));
            }
            checkPythonError();
            prevtext.set(utf, bytes);
        }
        return script.getLink();
    }
private:
    static StringBuffer &wrapPythonText(StringBuffer &out, const char *in)
    {
        out.append("def __user__():\n  ");
        char c;
        while ((c = *in++) != '\0')
        {
            out.append(c);
            if (c=='\n')
                out.append("  ");
        }
        out.append("\n__result__ = __user__()\n");
        return out;
    }
    GILstateWrapper GILState;
    OwnedPyObject module;
    OwnedPyObject script;
    StringAttr prevtext;
};

static __thread PythonThreadContext* threadContext;  // We reuse per thread, for speed
static __thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    if (threadContext)
    {
        delete threadContext;
        threadContext = NULL;
    }
    if (threadHookChain)
    {
        (*threadHookChain)();
        threadHookChain = NULL;
    }
}

// Use a global object to ensure that the Python interpreter is initialized on main thread

static class Python27GlobalState
{
public:
    Python27GlobalState()
    {
        pythonLibrary = (HINSTANCE) 0;
#ifndef _WIN32
        // If Py_Initialize is called when stdin is set to a directory, it calls exit()
        // We don't want that to happen - just disable Python support in such situations
        struct stat sb;
        if (fstat(fileno(stdin), &sb) == 0 && S_ISDIR(sb.st_mode))
        {
            initialized = false;
            return;
        }
#endif
#ifndef _WIN32
        // We need to ensure all symbols in the python2.6 so are loaded - due to bugs in some distro's python installations
        FILE *diskfp = fopen("/proc/self/maps", "r");
        if (diskfp)
        {
            char ln[_MAX_PATH];
            while (fgets(ln, sizeof(ln), diskfp))
            {
                if (strstr(ln, "libpython2"))
                {
                    const char *fullName = strchr(ln, '/');
                    if (fullName)
                    {
                        char * lf = (char *) strchr(fullName, '\n');
                        if (lf)
                        {
                            *lf = 0;
                            pythonLibrary = dlopen((char *)fullName, RTLD_NOW|RTLD_GLOBAL);
//                            DBGLOG("dlopen %s returns %"I64F"x", fullName, (__uint64) pythonLibrary);
                            break;
                        }
                    }
                }
            }
            fclose(diskfp);
        }
#endif
        // Initialize the Python Interpreter
        Py_Initialize();
        PyEval_InitThreads();
        tstate = PyEval_SaveThread();
        initialized = true;
    }
    ~Python27GlobalState()
    {
        if (threadContext)
            delete threadContext;   // The one on the main thread won't get picked up by the thread hook mechanism
        threadContext = NULL;
        if (initialized)
        {
            PyEval_RestoreThread(tstate);
            // Finish the Python Interpreter
            Py_Finalize();
        }
        if (pythonLibrary)
            FreeSharedObject(pythonLibrary);
    }
    bool isInitialized()
    {
        return initialized;
    }
protected:
    PyThreadState *tstate;
    bool initialized;
    HINSTANCE pythonLibrary;
} globalState;

static int countFields(const RtlFieldInfo * const * fields)
{
    unsigned count = 0;
    loop
    {
        if (!*fields)
            break;
        fields++;
        count++;
    }
    return count;
}

// Conversions from Python objects to ECL data

static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("pyembed: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", field->name->str());
    rtlFail(0, msg.str());
}

static bool getBooleanResult(const RtlFieldInfo *field, PyObject *obj)
{
    assertex(obj && obj != Py_None);
    if (!PyBool_Check(obj))
        typeError("boolean", field);
    return obj == Py_True;
}

static void getDataResult(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, void * &result)
{
    assertex(obj && obj != Py_None);
    if (!PyByteArray_Check(obj))
        typeError("bytearray", field);
    rtlStrToDataX(chars, result, PyByteArray_Size(obj), PyByteArray_AsString(obj));
}

static double getRealResult(const RtlFieldInfo *field, PyObject *obj)
{
    assertex(obj && obj != Py_None);
    if (!PyFloat_Check(obj))
        typeError("real", field);
    return PyFloat_AsDouble(obj);
}

static __int64 getSignedResult(const RtlFieldInfo *field, PyObject *obj)
{
    assertex(obj && obj != Py_None);
    __int64 ret;
    if (PyInt_Check(obj))
        ret = PyInt_AsUnsignedLongLongMask(obj);
    else if (PyLong_Check(obj))
        ret = (__int64) PyLong_AsLongLong(obj);
    else
        typeError("integer", field);
    return ret;
}

static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, PyObject *obj)
{
    assertex(obj && obj != Py_None);
    unsigned __int64 ret;
    if (PyInt_Check(obj))
        ret = PyInt_AsUnsignedLongLongMask(obj);
    else if (PyLong_Check(obj))
        ret =  (unsigned __int64) PyLong_AsUnsignedLongLong(obj);
    else
        typeError("integer", field);
    return ret;
}

static void getStringResult(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, char * &result)
{
    assertex(obj && obj != Py_None);
    if (PyString_Check(obj))
    {
        const char * text =  PyString_AsString(obj);
        checkPythonError();
        size_t lenBytes = PyString_Size(obj);
        rtlStrToStrX(chars, result, lenBytes, text);
    }
    else
        typeError("string", field);
}

static void getUTF8Result(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, char * &result)
{
    assertex(obj && obj != Py_None);
    if (PyUnicode_Check(obj))
    {
        OwnedPyObject utf8 = PyUnicode_AsUTF8String(obj);
        checkPythonError();
        size_t lenBytes = PyString_Size(utf8);
        const char * text =  PyString_AsString(utf8);
        checkPythonError();
        size32_t numchars = rtlUtf8Length(lenBytes, text);
        rtlUtf8ToUtf8X(chars, result, numchars, text);
    }
    else
        typeError("unicode string", field);
}

static void getSetResult(PyObject *obj, bool & isAllResult, size32_t & resultBytes, void * & result, int elemType, size32_t elemSize)
{
    // MORE - should probably recode to use the getResultDataset mechanism
    assertex(obj && obj != Py_None);
    if (!PyList_Check(obj) && !PySet_Check(obj))
        rtlFail(0, "pyembed: type mismatch - list or set expected");
    rtlRowBuilder out;
    size32_t outBytes = 0;
    byte *outData = NULL;
    OwnedPyObject iter = PyObject_GetIter(obj);
    OwnedPyObject elem;
    for (elem.setown(PyIter_Next(iter)); elem != NULL; elem.setown(PyIter_Next(iter)))
    {
        if (elemSize != UNKNOWN_LENGTH)
        {
            out.ensureAvailable(outBytes + elemSize);
            outData = out.getbytes() + outBytes;
            outBytes += elemSize;
        }
        switch ((type_t) elemType)
        {
        case type_int:
            rtlWriteInt(outData, pyembed::getSignedResult(NULL, elem), elemSize);
            break;
        case type_unsigned:
            rtlWriteInt(outData, pyembed::getUnsignedResult(NULL, elem), elemSize);
            break;
        case type_real:
            if (elemSize == sizeof(double))
                * (double *) outData = (double) pyembed::getRealResult(NULL, elem);
            else
            {
                assertex(elemSize == sizeof(float));
                * (float *) outData = (float) pyembed::getRealResult(NULL, elem);
            }
            break;
        case type_boolean:
            assertex(elemSize == sizeof(bool));
            * (bool *) outData = pyembed::getBooleanResult(NULL, elem);
            break;
        case type_string:
        case type_varstring:
        {
            if (!PyString_Check(elem))
                rtlFail(0, "pyembed: type mismatch - return value in list was not a STRING");
            const char * text =  PyString_AsString(elem);
            checkPythonError();
            size_t lenBytes = PyString_Size(elem);
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
            if (!PyUnicode_Check(elem))
                rtlFail(0, "pyembed: type mismatch - return value in list was not a unicode STRING");
            OwnedPyObject utf8 = PyUnicode_AsUTF8String(elem);
            checkPythonError();
            size_t lenBytes = PyString_Size(utf8);
            const char * text =  PyString_AsString(utf8);
            checkPythonError();
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
        case type_data:
        {
            if (!PyByteArray_Check(elem))
                rtlFail(0, "pyembed: type mismatch - return value in list was not a bytearray");
            size_t lenBytes = PyByteArray_Size(elem);  // Could check does not overflow size32_t
            const char *data = PyByteArray_AsString(elem);
            if (elemSize == UNKNOWN_LENGTH)
            {
                out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                outData = out.getbytes() + outBytes;
                * (size32_t *) outData = lenBytes;
                rtlStrToData(lenBytes, outData+sizeof(size32_t), lenBytes, data);
                outBytes += lenBytes + sizeof(size32_t);
            }
            else
                rtlStrToData(elemSize, outData, lenBytes, data);
            break;
        }
        default:
            rtlFail(0, "pyembed: type mismatch - unsupported return type");
            break;
        }
        checkPythonError();
    }
    isAllResult = false;
    resultBytes = outBytes;
    result = out.detachdata();
}

static void getUnicodeResult(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, UChar * &result)
{
    assertex(obj && obj != Py_None);
    if (PyUnicode_Check(obj))
    {
        OwnedPyObject utf8 = PyUnicode_AsUTF8String(obj);
        checkPythonError();
        size_t lenBytes = PyString_Size(utf8);
        const char * text =  PyString_AsString(utf8);
        checkPythonError();
        size32_t numchars = rtlUtf8Length(lenBytes, text);
        rtlUtf8ToUnicodeX(chars, result, numchars, text);
    }
    else
        typeError("unicode string", field);
}

// A PythonRowBuilder object is used to construct an ECL row from a python object

class PythonRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    PythonRowBuilder(PyObject *_row)
    : iter(NULL), elem(_row)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        bool ret = pyembed::getBooleanResult(field, elem);
        nextField();
        return ret;
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        pyembed::getDataResult(field, elem, len, result);
        nextField();
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        double ret = pyembed::getRealResult(field, elem);
        nextField();
        return ret;
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        __int64 ret = pyembed::getSignedResult(field, elem);
        nextField();
        return ret;
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        unsigned __int64 ret = pyembed::getUnsignedResult(field, elem);
        nextField();
        return ret;
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        pyembed::getStringResult(field, elem, chars, result);
        nextField();
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        pyembed::getUTF8Result(field, elem, chars, result);
        nextField();
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        pyembed::getUnicodeResult(field, elem, chars, result);
        nextField();
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        double ret = pyembed::getRealResult(field, elem);
        value.setReal(ret);
        nextField();
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false;  // No concept of an 'all' set in Python
        assertex(elem && elem != Py_None);
        if (!PyList_Check(elem) && !PySet_Check(elem))
            typeError("list or set", field);
        iterStack.append(iter.getClear());
        iter.setown(PyObject_GetIter(elem));
        nextField();
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        return elem != NULL;
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        if (PyList_Check(elem))
        {
            iterStack.append(iter.getClear());
            iter.setown(PyObject_GetIter(elem));
            nextField();
        }
        else
            typeError("list", field);
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
        // Expect to see a tuple here, or possibly (if the ECL record has a single field), an arbitrary scalar object
        // If it's a tuple, we push it onto our stack as the active object
        iterStack.append(iter.getClear());
        if (PyTuple_Check(elem))
        {
            iter.setown(PyObject_GetIter(elem));
            nextField();
        }
        else if (countFields(field->type->queryFields())==1)
        {
            // Python doesn't seem to support the concept of a tuple containing a single element.
            // If we are expecting a single field in our row, then the 'tuple' layer will be missing
            // NOTE - we don't call nextField here, and iter is set to null by the getClear() above.
            // elem will be valid for one field only
        }
        else
        {
            typeError("tuple", field);
        }
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        return elem != NULL;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        iter.setown((PyObject *) iterStack.pop());
        nextField();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        iter.setown((PyObject *) iterStack.pop());
        nextField();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        iter.setown((PyObject *) iterStack.pop());
        nextField();
    }
protected:
    void nextField()
    {
        if (iter)
            elem.setown(PyIter_Next(iter));
        else
            elem = NULL;
        checkPythonError();
    }
    OwnedPyObject iter;
    OwnedPyObject elem;
    PointerArray iterStack;
};

// Each call to a Python function will use a new Python27EmbedFunctionContext object
// This takes care of ensuring that the Python GIL is locked while we are executing python code,
// and released when we are not

class Python27EmbedContextBase : public CInterface, implements IEmbedFunctionContext, implements IRowStream
{
public:
    IMPLEMENT_IINTERFACE;

    Python27EmbedContextBase(PythonThreadContext *_sharedCtx)
    : sharedCtx(_sharedCtx), resultIterator(NULL)
    {
        PyEval_RestoreThread(sharedCtx->threadState);
        locals.setown(PyDict_New());
        globals.setown(PyDict_New());
        PyDict_SetItemString(locals, "__builtins__", PyEval_GetBuiltins());  // required for import to work
    }
    ~Python27EmbedContextBase()
    {
        // We need to clear these before calling savethread, or we won't own the GIL
        locals.clear();
        globals.clear();
        result.clear();
        script.clear();
        sharedCtx->threadState = PyEval_SaveThread();
    }

    virtual bool getBooleanResult()
    {
        return pyembed::getBooleanResult(NULL, result);
    }
    virtual void getDataResult(size32_t &__chars, void * &__result)
    {
        pyembed::getDataResult(NULL, result, __chars, __result);
    }
    virtual double getRealResult()
    {
        return pyembed::getRealResult(NULL, result);
    }
    virtual __int64 getSignedResult()
    {
        return pyembed::getSignedResult(NULL, result);
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return pyembed::getUnsignedResult(NULL, result);
    }
    virtual void getStringResult(size32_t &__chars, char * &__result)
    {
        pyembed::getStringResult(NULL, result, __chars, __result);
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        pyembed::getUTF8Result(NULL, result, __chars, __result);
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        pyembed::getUnicodeResult(NULL, result, __chars, __result);
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        pyembed::getSetResult(result, __isAllResult, __resultBytes, __result, elemType, elemSize);
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        assertex(result && result != Py_None);
        if (!PyList_Check(result) && !PyGen_Check(result))  // MORE - should I remove this check, and just say if it is iterable, it's good?
            typeError("list or generator", NULL);
        resultIterator.setown(PyObject_GetIter(result));
        checkPythonError();
        resultAllocator.set(_resultAllocator);
        return LINK(this);
    }
    virtual const void *nextRow()
    {
        assertex(resultAllocator);
        assertex(resultIterator);
        OwnedPyObject row = PyIter_Next(resultIterator);
        if (!row)
            return NULL;
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        PythonRowBuilder pyRowBuilder(row);
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, pyRowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        addArg(name, PyBool_FromLong(val ? 1 : 0));
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        addArg(name, PyByteArray_FromStringAndSize((const char *) val, len));
    }
    virtual void bindRealParam(const char *name, double val)
    {
        addArg(name, PyFloat_FromDouble(val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        addArg(name, PyLong_FromLongLong(val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        addArg(name, PyLong_FromUnsignedLongLong(val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        addArg(name, PyString_FromStringAndSize(val, len));
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        addArg(name, PyString_FromString(val));
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        size32_t sizeBytes = rtlUtf8Size(chars, val);
        PyObject *vval = PyUnicode_FromStringAndSize(val, sizeBytes);   // NOTE - requires size in bytes not chars
        checkPythonError();
        addArg(name, vval);
    }

    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        // You don't really know what size Py_UNICODE is (varies from system to system), so go via utf8
        unsigned unicodeChars;
        char *unicode;
        rtlUnicodeToUtf8X(unicodeChars, unicode, chars, val);
        size32_t sizeBytes = rtlUtf8Size(unicodeChars, unicode);
        PyObject *vval = PyUnicode_FromStringAndSize(unicode, sizeBytes);   // NOTE - requires size in bytes not chars
        checkPythonError();
        addArg(name, vval);
        rtlFree(unicode);
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        if (isAll)
            rtlFail(0, "pyembed: Cannot pass ALL");
        type_t typecode = (type_t) elemType;
        const byte *inData = (const byte *) setData;
        const byte *endData = inData + totalBytes;
        OwnedPyObject vval = PyList_New(0);
        while (inData < endData)
        {
            OwnedPyObject thisElem;
            size32_t thisSize = elemSize;
            switch (typecode)
            {
            case type_int:
                thisElem.setown(PyLong_FromLongLong(rtlReadInt(inData, elemSize)));
                break;
            case type_unsigned:
                thisElem.setown(PyLong_FromUnsignedLongLong(rtlReadUInt(inData, elemSize)));
                break;
            case type_varstring:
            {
                size32_t numChars = strlen((const char *) inData);
                thisElem.setown(PyString_FromStringAndSize((const char *) inData, numChars));
                if (elemSize == UNKNOWN_LENGTH)
                    thisSize = numChars + 1;
                break;
            }
            case type_string:
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = * (size32_t *) inData;
                    inData += sizeof(size32_t);
                }
                thisElem.setown(PyString_FromStringAndSize((const char *) inData, thisSize));
                break;
            case type_real:
                if (elemSize == sizeof(double))
                    thisElem.setown(PyFloat_FromDouble(* (double *) inData));
                else
                    thisElem.setown(PyFloat_FromDouble(* (float *) inData));
                break;
            case type_boolean:
                assertex(elemSize == sizeof(bool));
                thisElem.setown(PyBool_FromLong(*(bool*)inData ? 1 : 0));
                break;
            case type_unicode:
            {
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = (* (size32_t *) inData) * sizeof(UChar); // NOTE - it's in chars...
                    inData += sizeof(size32_t);
                }
                unsigned unicodeChars;
                rtlDataAttr unicode;
                rtlUnicodeToUtf8X(unicodeChars, unicode.refstr(), thisSize / sizeof(UChar), (const UChar *) inData);
                size32_t sizeBytes = rtlUtf8Size(unicodeChars, unicode.getstr());
                thisElem.setown(PyUnicode_FromStringAndSize(unicode.getstr(), sizeBytes));   // NOTE - requires size in bytes not chars
                checkPythonError();
                break;
            }
            case type_utf8:
            {
                assertex (elemSize == UNKNOWN_LENGTH);
                size32_t numChars = * (size32_t *) inData;
                inData += sizeof(size32_t);
                thisSize = rtlUtf8Size(numChars, inData);
                thisElem.setown(PyUnicode_FromStringAndSize((const char *) inData, thisSize));   // NOTE - requires size in bytes not chars
                break;
            }
            case type_data:
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = * (size32_t *) inData;
                    inData += sizeof(size32_t);
                }
                thisElem.setown(PyByteArray_FromStringAndSize((const char *) inData, thisSize));
                break;
            }
            checkPythonError();
            inData += thisSize;
            PyList_Append(vval, thisElem);
        }
        addArg(name, vval.getLink());
    }

protected:
    virtual void addArg(const char *name, PyObject *arg) = 0;

    PythonThreadContext *sharedCtx;
    OwnedPyObject locals;
    OwnedPyObject globals;
    OwnedPyObject result;
    OwnedPyObject script;

    Linked<IEngineRowAllocator> resultAllocator;
    OwnedPyObject resultIterator;
};

class Python27EmbedScriptContext : public Python27EmbedContextBase
{
public:
    Python27EmbedScriptContext(PythonThreadContext *_sharedCtx, const char *options)
    : Python27EmbedContextBase(_sharedCtx)
    {
    }
    ~Python27EmbedScriptContext()
    {
    }

    virtual void importFunction(size32_t lenChars, const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        script.setown(sharedCtx->compileEmbeddedScript(lenChars, utf));
    }

    virtual void callFunction()
    {
        result.setown(PyEval_EvalCode((PyCodeObject *) script.get(), locals, globals));
        checkPythonError();
        if (!result || result == Py_None)
            result.set(PyDict_GetItemString(locals, "__result__"));
        if (!result || result == Py_None)
            result.set(PyDict_GetItemString(globals, "__result__"));
    }
protected:
    virtual void addArg(const char *name, PyObject *arg)
    {
        assertex(arg);
        PyDict_SetItemString(locals, name, arg);
        Py_DECREF(arg);
        checkPythonError();
    }
};

class Python27EmbedImportContext : public Python27EmbedContextBase
{
public:
    Python27EmbedImportContext(PythonThreadContext *_sharedCtx, const char *options)
    : Python27EmbedContextBase(_sharedCtx)
    {
        argcount = 0;
    }
    ~Python27EmbedImportContext()
    {
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        script.setown(sharedCtx->importFunction(lenChars, utf));
    }
    virtual void compileEmbeddedScript(size32_t len, const char *text)
    {
        throwUnexpected();
    }
    virtual void callFunction()
    {
        result.setown(PyObject_CallObject(script, args));
        checkPythonError();
    }
private:
    virtual void addArg(const char *name, PyObject *arg)
    {
        if (argcount)
            _PyTuple_Resize(args.ref(), argcount+1);
        else
            args.setown(PyTuple_New(1));
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), argcount++, arg);  // Note - 'steals' the arg reference
    }
    int argcount;
    OwnedPyObject args;
};

class Python27EmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        if (!threadContext)
        {
            if (!globalState.isInitialized())
                rtlFail(0, "Python not initialized");
            threadContext = new PythonThreadContext;
            threadHookChain = addThreadTermFunc(releaseContext);
        }
        if (isImport)
            return new Python27EmbedImportContext(threadContext, options);
        else
            return new Python27EmbedScriptContext(threadContext, options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new Python27EmbedContext;
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
