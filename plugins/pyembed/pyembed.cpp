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
#include "eclrtl.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] = {
    "Python2.7 Embed Helper 1.0.0",
    NULL };

static const char *version = "Python2.7 Embed Helper 1.0.0";

static const char * EclDefinition =
    "EXPORT Language := SERVICE\n"
    "  boolean getEmbedContext():cpp,pure,namespace='pyembed',entrypoint='getEmbedContext',prototype='IEmbedContext* getEmbedContext()';\n"
    "  boolean syntaxCheck(const varstring src):cpp,pure,namespace='pyembed',entrypoint='syntaxCheck';\n"
    "END;"
    "EXPORT getEmbedContext := Language.getEmbedContext;"
    "EXPORT syntaxCheck := Language.syntaxCheck;"
    "EXPORT boolean supportsImport := true;"
    "EXPORT boolean supportsScript := true;";

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
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_DLL_MODULE | PLUGIN_MULTIPLE_VERSIONS;
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
        PyErr_Fetch(pType.ref(), pType.ref(), pType.ref());
        OwnedPyObject errStr = PyObject_Str(err);
        OwnedPyObject valStr = PyObject_Str(pValue);
        PyErr_Clear();
        VStringBuffer errMessage("%s: %s", PyString_AsString(errStr), PyString_AsString(valStr));
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

    inline PyObject * importFunction(const char *text)
    {
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            // Name should be in the form module.function
            const char *funcname = strrchr(text, '.');
            if (!funcname)
                rtlFail(0, "Expected module.function");
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
                    PyList_Append(sys_path, new_path);
                    checkPythonError();
                }
            }
            module.setown(PyImport_ImportModule(modname));
            checkPythonError();
            PyObject *dict = PyModule_GetDict(module);  // this is a borrowed reference and does not need to be released
            script.set(PyDict_GetItemString(dict, funcname));
            checkPythonError();
            if (!script || !PyCallable_Check(script))
                rtlFail(0, "Object is not callable");
            prevtext.set(text);
        }
        return script.getLink();
    }

    inline PyObject *compileEmbeddedScript(const char *text)
    {
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            // Try compiling as a eval first... if that fails, try as a script.
            script.setown(Py_CompileString(text, "", Py_eval_input));
            if (!script)
            {
                PyErr_Clear();
                StringBuffer wrapped;
                wrapPythonText(wrapped, text);
                script.setown(Py_CompileString(wrapped, "<embed>", Py_file_input));
            }
            checkPythonError();
            prevtext.set(text);
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
    delete threadContext;
    threadContext = NULL;
    if (threadHookChain)
        (*threadHookChain)();
}

// Use a global object to ensure that the Python interpreter is initialized on main thread

static class Python27GlobalState
{
public:
    Python27GlobalState()
    {
        // Initialize the Python Interpreter
        Py_Initialize();
        PyEval_InitThreads();
        tstate = PyEval_SaveThread();
    }
    ~Python27GlobalState()
    {
        if (threadContext)
            delete threadContext;   // The one on the main thread won't get picked up by the thread hook mechanism
        threadContext = NULL;
        PyEval_RestoreThread(tstate);
        // Finish the Python Interpreter
        Py_Finalize();
    }
protected:
    PyThreadState *tstate;
} globalState;

// Each call to a Python function will use a new Python27EmbedFunctionContext object
// This takes care of ensuring that the Python GIL is locked while we are executing python code,
// and released when we are not

class Python27EmbedContextBase : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    Python27EmbedContextBase(PythonThreadContext *_sharedCtx)
    : sharedCtx(_sharedCtx)
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
        assertex(result);
        if (!PyBool_Check(result))
            throw MakeStringException(MSGAUD_user, 0, "pyembed: Type mismatch on result");
        return result == Py_True;
    }
    virtual double getRealResult()
    {
        assertex(result);
        return (__int64) PyFloat_AsDouble(result);
    }
    virtual __int64 getSignedResult()
    {
        assertex(result);
        return (__int64) PyLong_AsLongLong(result);
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return (__int64) PyLong_AsUnsignedLongLong(result);
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        assertex(result);
        __len = PyString_Size(result);
        const char * chars =  PyString_AsString(result);
        checkPythonError();
        __result = (char *)rtlMalloc(__len);
        memcpy(__result, chars, __len);
    }

protected:
    PythonThreadContext *sharedCtx;
    OwnedPyObject locals;
    OwnedPyObject globals;
    OwnedPyObject result;
    OwnedPyObject script;
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
    virtual void bindBooleanParam(const char *name, bool val)
    {
        OwnedPyObject vval = PyBool_FromLong(val ? 1 : 0);
        PyDict_SetItemString(locals, name, vval);
    }
    virtual void bindRealParam(const char *name, double val)
    {
        OwnedPyObject vval = PyFloat_FromDouble(val);
        PyDict_SetItemString(locals, name, vval);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        OwnedPyObject vval = PyLong_FromLongLong(val);
        PyDict_SetItemString(locals, name, vval);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        OwnedPyObject vval = PyLong_FromUnsignedLongLong(val);
        PyDict_SetItemString(locals, name, vval);
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        OwnedPyObject vval = PyString_FromStringAndSize(val, len);
        PyDict_SetItemString(locals, name, vval);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        OwnedPyObject vval = PyString_FromString(val);
        PyDict_SetItemString(locals, name, vval);
    }

    virtual void importFunction(const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(const char *text)
    {
        script.setown(sharedCtx->compileEmbeddedScript(text));
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
    virtual void bindBooleanParam(const char *name, bool val)
    {
        addArg(PyBool_FromLong(val ? 1 : 0));
    }
    virtual void bindRealParam(const char *name, double val)
    {
        addArg(PyFloat_FromDouble(val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        addArg(PyLong_FromLongLong(val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        addArg(PyLong_FromUnsignedLongLong(val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        addArg(PyString_FromStringAndSize(val, len));
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        addArg(PyString_FromString(val));
    }

    virtual void importFunction(const char *text)
    {
        script.setown(sharedCtx->importFunction(text));
    }
    virtual void compileEmbeddedScript(const char *text)
    {
        throwUnexpected();
    }
    virtual void callFunction()
    {
        result.setown(PyObject_CallObject(script, args));
        checkPythonError();
    }
private:
    void addArg(PyObject *arg)
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
