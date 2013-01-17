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
    "export getEmbedContext := Language.getEmbedContext;"
    "export syntaxCheck := Language.syntaxCheck;";

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
};

// call checkPythonError to throw an exception if Python error state is set

static void checkPythonError()
{
    PyObject* err = PyErr_Occurred();
    if (err)
    {
        OwnedPyObject errStr = PyObject_Str(err);
        PyErr_Clear();
        rtlFail(0, PyString_AsString(errStr));
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
        locals.setown(PyDict_New());
        globals.setown(PyDict_New());
    }
    ~PythonThreadContext()
    {
        locals.clear();
        globals.clear();
        script.clear();
        result.clear();
        PyEval_RestoreThread(threadState);
    }

    inline void bindRealParam(const char *name, double val)
    {
        OwnedPyObject vval = PyFloat_FromDouble(val);
        PyDict_SetItemString(locals, name, vval);
    }
    inline void bindSignedParam(const char *name, __int64 val)
    {
        OwnedPyObject vval = PyLong_FromLongLong(val);
        PyDict_SetItemString(locals, name, vval);
    }
    inline void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        OwnedPyObject vval = PyLong_FromUnsignedLongLong(val);
        PyDict_SetItemString(locals, name, vval);
    }
    inline void bindStringParam(const char *name, size32_t len, const char *val)
    {
        OwnedPyObject vval = PyString_FromStringAndSize(val, len);
        PyDict_SetItemString(locals, name, vval);
    }
    inline void bindVStringParam(const char *name, const char *val)
    {
        OwnedPyObject vval = PyString_FromString(val);
        PyDict_SetItemString(locals, name, vval);
    }

    inline double getRealResult()
    {
        assertex(result);
        return (__int64) PyFloat_AsDouble(result);
    }
    inline __int64 getSignedResult()
    {
        assertex(result);
        return (__int64) PyLong_AsLongLong(result);
    }
    inline unsigned __int64 getUnsignedResult()
    {
        return (__int64) PyLong_AsUnsignedLongLong(result);
    }
    inline void getStringResult(size32_t &__len, char * &__result)
    {
        assertex(result);
        __len = PyString_Size(result);
        const char * chars =  PyString_AsString(result);
        checkPythonError();
        __result = (char *)rtlMalloc(__len);
        memcpy(__result, chars, __len);
    }

    inline void compileEmbeddedScript(const char *text)
    {
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            script.setown(Py_CompileString(text, "", Py_eval_input));
            checkPythonError();
            prevtext.set(text);
        }

    }
    inline void callFunction()
    {
        checkPythonError();
        result.setown(PyEval_EvalCode((PyCodeObject *) script.get(), locals, globals));
        checkPythonError();
    }
private:
    GILstateWrapper GILState;
    OwnedPyObject locals;
    OwnedPyObject globals;
    OwnedPyObject script;
    OwnedPyObject result;
    StringAttr prevtext;
};

// Each call to a Python function will use a new Python27EmbedFunctionContext object
// This takes care of ensuring that the Python GIL is locked while we are executing python code,
// and released when we are not

class Python27EmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    Python27EmbedFunctionContext(PythonThreadContext *_sharedCtx)
    : sharedCtx(_sharedCtx)
    {
        PyEval_RestoreThread(sharedCtx->threadState);
    }
    ~Python27EmbedFunctionContext()
    {
        sharedCtx->threadState = PyEval_SaveThread();
    }
    virtual void bindRealParam(const char *name, double val)
    {
        return sharedCtx->bindRealParam(name, val);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        return sharedCtx->bindSignedParam(name, val);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        return sharedCtx->bindUnsignedParam(name, val);
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        return sharedCtx->bindStringParam(name, len, val);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        return sharedCtx->bindVStringParam(name, val);
    }

    virtual double getRealResult()
    {
        return sharedCtx->getRealResult();
    }
    virtual __int64 getSignedResult()
    {
        return sharedCtx->getSignedResult();
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return sharedCtx->getUnsignedResult();
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        sharedCtx->getStringResult(__len, __result);
    }

    virtual void compileEmbeddedScript(const char *text)
    {
        sharedCtx->compileEmbeddedScript(text);
    }
    virtual void callFunction()
    {
        sharedCtx->callFunction();
    }
private:
    PythonThreadContext *sharedCtx;
};

__thread PythonThreadContext* threadContext;  // We reuse per thread, for speed
__thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    delete threadContext;
    if (threadHookChain)
        (*threadHookChain)();
}

static class Python27EmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    Python27EmbedContext()
    {
        // Initialize the Python Interpreter
        Py_Initialize();
        PyEval_InitThreads();
        tstate = PyEval_SaveThread();
        Link();  // Deliberately 'leak' in order to avoid freeing this global object prematurely
    }
    ~Python27EmbedContext()
    {
        PyEval_RestoreThread(tstate);
        // Finish the Python Interpreter
        Py_Finalize();
    }
    virtual IEmbedFunctionContext *createFunctionContext()
    {
        if (!threadContext)
        {
            threadContext = new PythonThreadContext;
            threadHookChain = addThreadTermFunc(releaseContext);
        }
        return new Python27EmbedFunctionContext(threadContext);
    }
protected:
    PyThreadState *tstate;
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
