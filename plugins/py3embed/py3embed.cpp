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


#ifdef _WIN32
// There's an issue with Python redefining ssize_t resulting in errors - hide their definition
#define ssize_t python_ssize_t
#include "Python.h"
#undef ssize_t
#else
#define register
#include "Python.h"
#endif

#include "platform.h"
#include "frameobject.h"
#include "jexcept.hpp"
#include "jutil.hpp"
#include "jthread.hpp"
#include "jregexp.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield.hpp"
#include "nbcd.hpp"
#include "roxiemem.hpp"
#include "enginecontext.hpp"
#include <regex>

#if PY_MAJOR_VERSION >=3
  #define Py_TPFLAGS_HAVE_ITER 0
#endif

#if PY_MINOR_VERSION < 7
  #define USE_CUSTOM_NAMEDTUPLES
#endif

static const char * compatibleVersions[] = {
    "Python3.x Embed Helper 1.0.0",
    NULL };

static const char *version = "Python3.x Embed Helper 1.0.0";

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
    pb->moduleName = "python3";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "Python3.x Embed Helper";
    return true;
}

namespace py3embed {

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
    inline void set(PyObject *_ptr)         { if (_ptr) Py_INCREF(_ptr); clear(); ptr = _ptr; }
    inline PyObject *getLink()              { if (ptr) Py_INCREF(ptr); return ptr;}
    inline PyObject **ref()                 { return &ptr; }
};

template <class X>
class OwnedPyX
{
    X *ptr;
public:
    inline OwnedPyX<X>() : ptr(NULL) {}
    inline OwnedPyX<X>(X *_ptr) : ptr(_ptr) {}
    inline ~OwnedPyX<X>()            { if (ptr) Py_DECREF(ptr); }
    inline X * get() const           { return ptr; }
    inline X * getClear()            { PyObject *ret = ptr; ptr = NULL; return ret; }
    inline X * operator -> () const  { return ptr; }
    inline operator X *() const      { return ptr; }
    inline void clear()              { if (ptr) Py_DECREF(ptr); ptr = NULL; }
    inline void setown(X *_ptr)      { clear(); ptr = _ptr; }
    inline void set(X *_ptr)         { if (_ptr) Py_INCREF(_ptr); clear(); ptr = _ptr; }
    inline X *getLink()              { if (ptr) Py_INCREF(ptr); return ptr;}
    inline X **ref()                 { return &ptr; }
};


__declspec(noreturn) static void failx(const char *msg, ...) __attribute__((format(printf, 1, 2), noreturn));

static void failx(const char *message, ...)
{
    va_list args;
    va_start(args,message);
    StringBuffer msg;
    msg.append("pyembed: ").valist_appendf(message,args);
    va_end(args);
    rtlFail(0, msg.str());
}

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
        assertex(PyUnicode_Check(valStr));
        const char *text = PyUnicode_AsUTF8AndSize(valStr, NULL);
        failx("%s", text);
    }
}

#ifdef USE_CUSTOM_NAMEDTUPLES
// In Python3.6 we can't use the standard namedtuple if we have > 255 params. So we need to use our own based on similar techniques.
// The code below is copied from the Python 3.6 collections module source, with minor modifications:
// - pass in the initial tuple values as a tuple rather than as separate parameters, to avoid the 256 param limit
// - remove some options we were not using

static constexpr const char * _py36code = R"!!(
import sys as _sys
from keyword import iskeyword as _iskeyword
_class_template = """\
from builtins import property as _property, tuple as _tuple
from operator import itemgetter as _itemgetter
from collections import OrderedDict
class {typename}(tuple):
    '{typename}({arg_list})'
    __slots__ = ()
    _fields = {field_names!r}
    def __new__(_cls, t):
        'Create new instance of {typename}({arg_list})'
        return _tuple.__new__(_cls, t)
    @classmethod
    def _make(cls, iterable, new=tuple.__new__, len=len):
        'Make a new {typename} object from a sequence or iterable'
        result = new(cls, iterable)
        if len(result) != {num_fields:d}:
            raise TypeError('Expected {num_fields:d} arguments, got %d' % len(result))
        return result
    def _replace(_self, **kwds):
        'Return a new {typename} object replacing specified fields with new values'
        result = _self._make(map(kwds.pop, {field_names!r}, _self))
        if kwds:
            raise ValueError('Got unexpected field names: %r' % list(kwds))
        return result
    def __repr__(self):
        'Return a nicely formatted representation string'
        return self.__class__.__name__ + '({repr_fmt})' % self
    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values.'
        return OrderedDict(zip(self._fields, self))
    def __getnewargs__(self):
        'Return self as a plain tuple.  Used by copy and pickle.'
        return tuple(self)
{field_defs}
"""

_repr_template = '"{name}=%r";'

_field_template = '''\
    {name} = _property(_itemgetter({index:d}), doc='Alias for field number {index:d}')
'''

def namedtuple(typename, field_names):

    # Validate the field names, and replace any invalid ones with a valid name.
    field_names = field_names.replace(',', ' ').split()
    field_names = list(map(str, field_names))
    typename = str(typename)
    for index, name in enumerate(field_names):
        if (not name.isidentifier()
            or _iskeyword(name)
            or name.startswith('_')):
            field_names[index] = '_%d' % index

    # Fill-in the class template
    class_definition = _class_template.format(
        typename = typename,
        field_names = tuple(field_names),
        num_fields = len(field_names),
        arg_list = repr(tuple(field_names)).replace("'", "")[1:-1],
        repr_fmt = ', '.join(_repr_template.format(name=name)
                             for name in field_names),
        field_defs = '\n'.join(_field_template.format(index=index, name=name)
                               for index, name in enumerate(field_names))
    )

    # Execute the template string in a temporary namespace and support
    # tracing utilities by setting a value for frame.f_globals['__name__']
    namespace = dict(__name__='namedtuple_%s' % typename)
    exec(class_definition, namespace)
    result = namespace[typename]

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in environments where
    # sys._getframe is not defined (Jython for example) or sys._getframe is not
    # defined for arguments greater than 0 (IronPython), or where the user has
    # specified a particular module.
    module = None
    try:
        module = _sys._getframe(1).f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass
    if module is not None:
        result.__module__ = module

    return result
)!!";

#endif

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
// Note that we assume that a thread is not shared between workunits/queries

class PythonThreadContext
{
public:
    PyThreadState *threadState;
public:
    PythonThreadContext()
    {
        threadState = PyEval_SaveThread();
        lrutype = NULL;
    }
    ~PythonThreadContext()
    {
        PyEval_RestoreThread(threadState);
        script.clear();
        module.clear();
        lru.clear();
    }

    void addManifestFiles(ICodeContext *codeCtx);

    PyObject * importFunction(ICodeContext *codeCtx, size32_t lenChars, const char *utf)
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
            addManifestFiles(codeCtx);
            StringBuffer modname(funcname-text, text);
            funcname++;  // skip the '.'
            // If the modname is preceded by a path, add it temporarily to the Python path before importing
            bool addedPath = false;
            PyObject *sysPath = PySys_GetObject((char *) "path");
            if (!sysPath)
                rtlFail(0, "pyembed: sys.path returned null");
            OwnedPyObject newpath;
            const char *pathsep = strrchr(modname, PATHSEPCHAR);
            if (pathsep)
            {
                StringBuffer path(pathsep-modname, modname);
                modname.remove(0, 1+pathsep-modname);
                newpath.setown(PyUnicode_FromString(path));
                Py_ssize_t found = PySequence_Index(sysPath, newpath);
                if (found == (Py_ssize_t)-1)
                {
                    PyErr_Clear();
                    PyList_Insert(sysPath, 0, newpath);
                    addedPath = true;
                }
                checkPythonError();
            }
            module.setown(PyImport_ImportModule(modname));
            checkPythonError();
            if (pathsep)
            {
                // Immediately remove the temporary location from the path (if we added it),
                // and the just-imported module from the system cache,
                // otherwise other code that imports similar name from other location fails.
                if (addedPath)
                {
                    Py_ssize_t found = PySequence_Index(sysPath, newpath);  // Very likely to be zero, but should we assume? You could argue we should restore path to state prior to import, whatever
                    if (found != (Py_ssize_t)-1)
                        PySequence_DelItem(sysPath, found);
                    else
                        PyErr_Clear();
                }
                PyObject *sysModules = PySys_GetObject((char *) "modules");
                DBGLOG("Unloading module %s", modname.str());
                OwnedPyObject pyMod = PyUnicode_FromString(modname);
                PyDict_DelItem(sysModules, pyMod);
                checkPythonError();
            }
            PyObject *dict = PyModule_GetDict(module);  // this is a borrowed reference and does not need to be released
            script.set(PyDict_GetItemString(dict, funcname));
            checkPythonError();
            if (!script || !PyCallable_Check(script))
                rtlFail(0, "pyembed: Object is not callable");
            prevtext.set(text);
        }
        return script.getLink();
    }

    PyObject *compileEmbeddedScript(ICodeContext *codeCtx, size32_t lenChars, const char *utf, const char *argstring);
    PyObject *getNamedTupleType(const RtlTypeInfo *type);
private:
    GILstateWrapper GILState;
    OwnedPyObject module;
    OwnedPyObject script;
    OwnedPyObject lru;
    const RtlTypeInfo *lrutype;
    StringAttr prevtext;
    bool manifestAdded = false;
};

static __thread PythonThreadContext* threadContext;  // We reuse per thread, for speed

static bool releaseContext(bool isPooled)
{
    if (threadContext)
    {
        delete threadContext;
        threadContext = NULL;
    }
    return false;
}

// Use a global object to ensure that the Python interpreter is initialized on main thread

static HINSTANCE keepLoadedHandle;

static class Python3xGlobalState
{
public:
    Python3xGlobalState()
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
        StringBuffer py2modname;
        if  (findLoadedModule(py2modname, "libpy2embed."))
        {
            initialized = false;
            multiPython = true;
            return;
        }
#endif
#ifndef _WIN32
        // We need to ensure all symbols in the python3.x so are loaded - due to bugs in some distro's python installations
        // However this will likely break python2.
        // Therefore on systems where both are present, do NOT do this - people using centos systems that suffer from issue
        // https://bugs.centos.org/view.php?id=6063 will need to choose which version of python plugin to install but not both

        StringBuffer modname;
        if  (findLoadedModule(modname, "libpython3."))
            pythonLibrary = dlopen(modname.str(), RTLD_NOW|RTLD_GLOBAL);
#endif
        // Initialize the Python Interpreter
        Py_Initialize();
        const wchar_t *argv[] = { nullptr };
        PySys_SetArgvEx(0, (wchar_t **) argv, 0);
        PyEval_InitThreads();
        preservedScopes.setown(PyDict_New());
        tstate = PyEval_SaveThread();
        skipPythonCleanup = queryEnvironmentConf().getPropBool("skipPythonCleanup", true);
        initialized = true;
    }
    ~Python3xGlobalState()
    {
        if (threadContext)
            delete threadContext;   // The one on the main thread won't get picked up by the thread hook mechanism
        threadContext = NULL;
        if (initialized && !skipPythonCleanup)
        {
            PyEval_RestoreThread(tstate);
            // Finish the Python Interpreter
            namedtuple.clear();
            namedtupleTypes.clear();
            compiledScripts.clear();
            preservedScopes.clear();
            Py_Finalize();
            if (pythonLibrary)
                FreeSharedObject(pythonLibrary);
        }
        else
        {
            // Need to avoid releasing the associated py objects when these members destructors are called.
            namedtuple.getClear();
            namedtupleTypes.getClear();
            compiledScripts.getClear();
            preservedScopes.getClear();

        }
    }
    void checkInitialized()
    {
        if (multiPython)
            rtlFail(0, "Python3 not initialized as Python2 already loaded");
        else if (!initialized)
            rtlFail(0, "Python3 not initialized");
    }
    bool isInitialized()
    {
        return initialized;
    }
    PyFrameObject *pushDummyFrame()
    {
        PyThreadState* threadstate = PyThreadState_GET();
        if (!threadstate->frame)
        {
            OwnedPyObject globals = PyDict_New();
            OwnedPyObject locals = PyDict_New();
            OwnedPyX<PyCodeObject> code = PyCode_NewEmpty("<dummy>","<dummy>", 0);
            checkPythonError();
            PyFrameObject *frame = PyFrame_New(threadstate, code, globals, locals);
            checkPythonError();
            threadstate->frame = frame;
            return frame;
        }
        return NULL;
    }

    void popDummyFrame(PyFrameObject *frame)
    {
        PyThreadState* threadstate = PyThreadState_GET();
        if (threadstate->frame == frame)
            threadstate->frame = NULL;
    }


    PyObject *getActivityContextTupleType()
    {
        // Note - we do not need (and must not have) a lock protecting this. It is protected by the Python GIL,
        // and if we add our own lock we are liable to deadlock as the code within Py_CompileStringFlags may
        // temporarily release then re-acquire the GIL.
        if (!activityContextTupleType)
            activityContextTupleType.setown(getNamedTupleType("isLocal,numSlaves,numStrands,slave,strand"));
        return activityContextTupleType.get();
    }

    PyObject *getNamedTupleType(const char *names)
    {
        // It seems the customized namedtuple types leak, and they are slow to create, so take care to reuse
        // Note - we do not need (and must not have) a lock protecting this. It is protected by the Python GIL,
        // and if we add our own lock we are liable to deadlock as the code within Py_CompileStringFlags may
        // temporarily release then re-acquire the GIL.
        if (!namedtuple)
        {
            namedtupleTypes.setown(PyDict_New());
#ifdef USE_CUSTOM_NAMEDTUPLES
            OwnedPyObject temp_namespace(PyDict_New());
            PyDict_SetItemString(temp_namespace, "__builtins__",  PyEval_GetBuiltins());  // required for import to work
            checkPythonError();
            OwnedPyObject ran = PyRun_String(_py36code, Py_file_input, temp_namespace, temp_namespace);
            checkPythonError();
            namedtuple.set(PyDict_GetItemString(temp_namespace, "namedtuple"));   // NOTE - returns borrowed reference
#else
            OwnedPyObject pName = PyUnicode_FromString("collections");
            OwnedPyObject collections = PyImport_Import(pName);
            checkPythonError();
            namedtuple.setown(PyObject_GetAttrString(collections, "namedtuple"));
#endif
            checkPythonError();
            assertex(PyCallable_Check(namedtuple));
        }

        OwnedPyObject pnames = PyUnicode_FromString(names);
        OwnedPyObject mynamedtupletype;
        checkPythonError();
        mynamedtupletype.set(PyDict_GetItem(namedtupleTypes, pnames));   // NOTE - returns borrowed reference
        if (!mynamedtupletype)
        {
            OwnedPyObject recname = PyUnicode_FromString("namerec");     // MORE - do we care what the name is?
            OwnedPyObject ntargs = PyTuple_Pack(2, recname.get(), pnames.get());
            checkPythonError();
            OwnedPyX<PyFrameObject> frame = pushDummyFrame();
            mynamedtupletype.setown(PyObject_CallObject(namedtuple, ntargs));
            popDummyFrame(frame);
            checkPythonError();
            PyDict_SetItem(namedtupleTypes, pnames, mynamedtupletype);
        }
        checkPythonError();
        assertex(PyCallable_Check(mynamedtupletype));
        return mynamedtupletype.getClear();
    }

    PyObject *getNamedTupleType(const RtlTypeInfo *type)
    {
        const RtlFieldInfo * const *fields = type->queryFields();
        if (!fields && type->queryChildType())
            fields = type->queryChildType()->queryFields();
        assertex(fields);
        StringBuffer names;
        while (*fields)
        {
            const RtlFieldInfo *field = *fields;
            if (names.length())
                names.append(',');
            names.append(field->name);
            fields++;
        }
        return getNamedTupleType(names.str());
    }

    StringBuffer & reformatCompilerError(StringBuffer &ret, const char *error, unsigned leadingLines)
    {
        // Errors from compiler tend to look like this:
        // "('invalid syntax', ('<embed>', 3, 12, '     sfsf ss fs dfs f sfs\n'))"
        const char pattern [] = "\\('(.*)', \\('.*', ([0-9]*), ([0-9]*), (.*)\\)\\)";
        // Hopefully there are no embedded quotes in the error message or the filename
        rtlCompiledStrRegex r;
        size32_t outlen;
        char * out = NULL;
        r.setPattern(pattern, false);
        r->replace(outlen, out, strlen(error), error, 2, "$2");
        if (outlen < strlen(error))
        {
            unsigned line = atoi(out);
            rtlFree(out);
            if (line > leadingLines)
                line--;
            r->replace(outlen, out, strlen(error), error, 13, ", $3): $1: $4");
            ret.appendf("(%d", line);
        }
        ret.append(outlen, out);
        rtlFree(out);
        return ret;
    }
    PyObject *compileScript(const char *text, const char *parameters)
    {
        // Note - we do not need (and must not have) a lock protecting this. It is protected by the Python GIL,
        // and if we add our own lock we are liable to deadlock as the code within Py_CompileStringFlags may
        // temporarily release then re-acquire the GIL.
        if (!compiledScripts)
            compiledScripts.setown(PyDict_New());
        OwnedPyObject code;
        code.set(PyDict_GetItemString(compiledScripts, text));
        if (!code)
        {
            unsigned leadingLines = (unsigned) -1;  // Number of lines from input that have not been offset by 1 line in input to compiler
            code.setown(Py_CompileString(text, "", Py_eval_input));   // try compiling as simple expression...
            if (!code)
            {
                PyErr_Clear();
                PyCompilerFlags flags = { PyCF_SOURCE_IS_UTF8 };
                code.setown(Py_CompileStringFlags(text, "<embed>", Py_file_input, &flags));  // try compiling as global code
                if (!code)
                {
                    PyErr_Clear();
                    StringBuffer wrapped;
                    wrapPythonText(wrapped, text, parameters, leadingLines);
                    code.setown(Py_CompileStringFlags(wrapped, "<embed>", Py_file_input, &flags)); // try compiling as a function body
                }
            }
            PyObject* err = PyErr_Occurred();
            if (err)
            {
                OwnedPyObject pType, pValue, pTraceBack;
                PyErr_Fetch(pType.ref(), pValue.ref(), pTraceBack.ref());
                OwnedPyObject valStr = PyObject_Str(pValue);
                PyErr_Clear();
                // We reformat the error message a little, to make it more helpful
                assertex(PyUnicode_Check(valStr));
                const char *errtext = PyUnicode_AsUTF8AndSize(valStr, NULL);
                StringBuffer msg;
                reformatCompilerError(msg, errtext, leadingLines);
                rtlFail(0, msg.str());
            }
            if (code)
                PyDict_SetItemString(compiledScripts, text, code);
        }
        return code.getClear();
    }
    PyObject *getNamedScope(const char *key, bool &isNew)
    {
        if (!preservedScopes)
            preservedScopes.setown(PyDict_New());
        OwnedPyObject scope;
        scope.set(PyDict_GetItemString(preservedScopes, key));
        if (!scope)
        {
            scope.setown(PyDict_New());
            PyDict_SetItemString(preservedScopes, key, scope);
            isNew = true;
        }
        else
            isNew = false;
        return scope.getClear();
    }
    void releaseNamedScope(const char *key)
    {
        if (preservedScopes)
        {
            PyDict_DelItemString(preservedScopes, key);
            PyErr_Clear();  // Should be present, but ignore the error if it is not
        }
    }
    static void unregister(const char *key);
    static void removePath(const char *file);
protected:
    static StringBuffer &wrapPythonText(StringBuffer &out, const char *in, const char *params, unsigned &leadingLines)
    {
        // Complicated by needing to keep future import lines outside defined function
        // Per python spec, a future statement must appear near the top of the module. The only lines that can appear before a future statement are:
        //   the module docstring (if any),
        //   comments,
        //   blank lines, and
        //   other future statements.
        // We don't attempt to parse the python to spot these - instead, we pull all lines up to and including the last future statement out to the global scope.
        // Because this is a little unsophisticated it will be fooled by code that includes things that look like future statements inside multiline strings.
        // I don't care.
        StringArray lines;
        lines.appendList(in, "\n", false);
        RegExpr expr("^ *from +__future__ +import ");
        leadingLines = 0;
        ForEachItemIn(idx, lines)
        {
            if (expr.find(lines.item(idx)))
                leadingLines = idx+1;
        }
        for (unsigned leadingLine = 0; leadingLine < leadingLines; leadingLine++)
            out.append(lines.item(leadingLine)).append('\n');
        out.appendf("def __user__(%s):\n", params);
        for (unsigned line = leadingLines; line < lines.length(); line++)
            out.append("  ").append(lines.item(line)).append('\n');
        out.appendf("__result__ = __user__(%s)\n", params);
        return out;
    }
    PyThreadState *tstate = nullptr;
    bool initialized = false;
    bool multiPython = false;
    bool skipPythonCleanup = true; // Tensorflow seems to often lockup in the python cleanup process.
    HINSTANCE pythonLibrary = 0;
    OwnedPyObject namedtuple;      // collections.namedtuple
    OwnedPyObject namedtupleTypes; // dictionary of return values from namedtuple()
    OwnedPyObject compiledScripts; // dictionary of previously compiled scripts
    OwnedPyObject preservedScopes; // dictionary of preserved scopes
    OwnedPyObject activityContextTupleType; // type used for activity context
} globalState;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    // Make sure we are never unloaded (as Python may crash if we are)
    // we do this by doing a dynamic load of the pyembed library
#ifdef _WIN32
    HINSTANCE me = GetModuleHandle("py3embed");
    if (me)
    {
        char helperLibraryName[_MAX_PATH];
        ::GetModuleFileName(me, helperLibraryName, _MAX_PATH);
        if (strstr(helperLibraryName, "py3embed"))
        {
            HINSTANCE h = LoadSharedObject(helperLibraryName, false, false);
            DBGLOG("LoadSharedObject returned %p", h);
        }
    }
#else
    StringBuffer modname;
    if (findLoadedModule(modname, "libpy3embed"))
    {
        keepLoadedHandle = LoadSharedObject(modname, false, false);
    }
#endif
    return true;
}

static void checkThreadContext()
{
    if (!threadContext)
    {
        globalState.checkInitialized();
        threadContext = new PythonThreadContext;
        addThreadTermFunc(releaseContext);
    }
}

void PythonThreadContext::addManifestFiles(ICodeContext *codeCtx)
{
    if (codeCtx && !manifestAdded) // MORE - this assumes we never reuse a thread for a different workunit, without the thread termination hooks having been called
    {
        manifestAdded = true;
        IEngineContext *engine = codeCtx->queryEngineContext();
        if (engine)
        {
            const StringArray &manifestModules = engine->queryManifestFiles("pyzip");
            if (manifestModules.length())
            {
                PyObject *sysPath = PySys_GetObject((char *) "path");
                if (!sysPath)
                    rtlFail(0, "pyembed: sys.path returned null");
                ForEachItemIn(idx, manifestModules)
                {
                    const char *path = manifestModules.item(idx);
                    DBGLOG("Manifest zip %s", path);
                    OwnedPyObject newPath = PyUnicode_FromString(path);
                    PyList_Insert(sysPath, 0, newPath);
                    checkPythonError();
                    engine->onTermination(Python3xGlobalState::removePath, manifestModules.item(idx), true);
                }
            }
        }
    }
}


PyObject *PythonThreadContext::getNamedTupleType(const RtlTypeInfo *type)
{
    if (!lru || (type!=lrutype))
    {
        lru.setown(globalState.getNamedTupleType(type));
        lrutype = type;
    }
    return lru.getLink();
}

PyObject *PythonThreadContext::compileEmbeddedScript(ICodeContext *codeCtx, size32_t lenChars, const char *utf, const char *argstring)
{
    size32_t bytes = rtlUtf8Size(lenChars, utf);
    StringBuffer text(bytes, utf);
    if (!prevtext || strcmp(text, prevtext) != 0)
    {
        prevtext.clear();
        text.stripChar('\r');
        addManifestFiles(codeCtx);
        script.setown(globalState.compileScript(text, argstring));
        prevtext.set(utf, bytes);
    }
    return script.getLink();
}

// Conversions from Python objects to ECL data

__declspec(noreturn) static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("pyembed: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", field->name);
    else
        msg.appendf(" for return value");
    rtlFail(0, msg.str());
}

static bool getBooleanResult(const RtlFieldInfo *field, PyObject *obj)
{
    if (obj && obj != Py_None)
    {
        if (PyBool_Check(obj))
            return obj == Py_True;
    }
    typeError("boolean", field);
}

static void getDataResult(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, void * &result)
{
    if (obj && obj != Py_None && PyByteArray_Check(obj))
        rtlStrToDataX(chars, result, PyByteArray_Size(obj), PyByteArray_AsString(obj));
    else
        typeError("bytearray", field);
}

static double getRealResult(const RtlFieldInfo *field, PyObject *obj)
{
    if (obj && obj != Py_None)
    {
        if (PyFloat_Check(obj))
            return PyFloat_AsDouble(obj);
    }
    typeError("real", field);
}

static __int64 getSignedResult(const RtlFieldInfo *field, PyObject *obj)
{
    if (obj && obj != Py_None)
    {
        if (PyLong_Check(obj))
            return (__int64) PyLong_AsLongLong(obj);
    }
    typeError("integer", field);
}

static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, PyObject *obj)
{
    if (obj && obj != Py_None)
    {
        if (PyLong_Check(obj))
            return (unsigned __int64) PyLong_AsUnsignedLongLong(obj);
    }
    typeError("integer", field);
}

static void getStringResult(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, char * &result)
{
    if (obj && obj != Py_None)
    {
        if (PyUnicode_Check(obj))
        {
            OwnedPyObject temp_bytes = PyUnicode_AsEncodedString(obj, ASCII_LIKE_CODEPAGE, "ignore");
            checkPythonError();
            const char * text =  PyBytes_AsString(temp_bytes);
            checkPythonError();
            size_t lenBytes = PyBytes_Size(temp_bytes);
            rtlStrToStrX(chars, result, lenBytes, text);
            return;
        }
        else if (PyBytes_Check(obj))
        {
            const char * text = PyBytes_AsString(obj);
            checkPythonError();
            size_t lenBytes = PyBytes_Size(obj);
            rtlStrToStrX(chars, result, lenBytes, text);
            return;
        }
    }
    typeError("string", field);
}

static void getUTF8Result(const RtlFieldInfo *field, PyObject *obj, size32_t &chars, char * &result)
{
    if (obj && obj != Py_None && PyUnicode_Check(obj))
    {
        Py_ssize_t lenBytes;
        const char *text = PyUnicode_AsUTF8AndSize(obj, &lenBytes);
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
    if (!obj || obj == Py_None || (!PyList_Check(obj) && !PySet_Check(obj)))
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
            rtlWriteInt(outData, py3embed::getSignedResult(NULL, elem), elemSize);
            break;
        case type_unsigned:
            rtlWriteInt(outData, py3embed::getUnsignedResult(NULL, elem), elemSize);
            break;
        case type_real:
            if (elemSize == sizeof(double))
                * (double *) outData = (double) py3embed::getRealResult(NULL, elem);
            else
            {
                assertex(elemSize == sizeof(float));
                * (float *) outData = (float) py3embed::getRealResult(NULL, elem);
            }
            break;
        case type_boolean:
            assertex(elemSize == sizeof(bool));
            * (bool *) outData = py3embed::getBooleanResult(NULL, elem);
            break;
        case type_string:
        case type_varstring:
        {
            if (!PyUnicode_Check(elem))
                rtlFail(0, "pyembed: type mismatch - return value in list was not a STRING");
            OwnedPyObject temp_bytes = PyUnicode_AsEncodedString(elem, ASCII_LIKE_CODEPAGE, "ignore");
            checkPythonError();
            const char * text =  PyBytes_AsString(temp_bytes);
            checkPythonError();
            size_t lenBytes = PyBytes_Size(temp_bytes);
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
            Py_ssize_t lenBytes;
            const char *text = PyUnicode_AsUTF8AndSize(elem, &lenBytes);
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
    if (obj && obj != Py_None && PyUnicode_Check(obj))
    {
        Py_ssize_t lenBytes;
        const char *text = PyUnicode_AsUTF8AndSize(obj, &lenBytes);
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
    : iter(NULL), elem(NULL), named(false)
    {
        pushback.set(_row);
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return py3embed::getBooleanResult(field, elem);
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        nextField(field);
        py3embed::getDataResult(field, elem, len, result);
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return py3embed::getRealResult(field, elem);
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return py3embed::getSignedResult(field, elem);
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return py3embed::getUnsignedResult(field, elem);
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        nextField(field);
        py3embed::getStringResult(field, elem, chars, result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        nextField(field);
        py3embed::getUTF8Result(field, elem, chars, result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        nextField(field);
        py3embed::getUnicodeResult(field, elem, chars, result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        nextField(field);
        double ret = py3embed::getRealResult(field, elem);
        value.setReal(ret);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        nextField(field);
        isAll = false;  // No concept of an 'all' set in Python
        if (!elem || elem == Py_None || (!PyList_Check(elem) && !PySet_Check(elem)))
            typeError("list or set", field);
        push();
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        nextField(NULL);
        pushback.setown(elem.getClear());
        return pushback != NULL;
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        nextField(field);
        if (!PyList_Check(elem))
            typeError("list", field);
        push();
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
        // Expect to see a tuple here, or possibly (if the ECL record has a single field), an arbitrary scalar object
        // If it's a tuple, we push it onto our stack as the active object
        nextField(field);
        if (!PyTuple_Check(elem))
        {
            if (countFields(field->type->queryFields())==1)
            {
                // Python doesn't seem to support the concept of a tuple containing a single element.
                // If we are expecting a single field in our row, then the 'tuple' layer will be missing
                elem.setown(PyTuple_Pack(1, elem.get()));
            }
            else
                typeError("tuple", field);
        }
        push();
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        nextField(NULL);
        pushback.setown(elem.getClear());
        return pushback != NULL;
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
        pop();
    }
protected:
    void pop()
    {
        iter.setown((PyObject *) iterStack.popGet());
        parent.setown((PyObject *) parentStack.popGet());
        named = namedStack.popGet();
        elem.clear();
    }
    void push()
    {
        iterStack.append(iter.getClear());
        parentStack.append(parent.getClear());
        namedStack.append(named);
        parent.set(elem);
        iter.setown(PyObject_GetIter(elem));
        named = isNamedTuple(elem);
        elem.clear();
    }
    bool isNamedTuple(PyObject *obj)
    {
        return PyObject_HasAttrString((PyObject *) obj->ob_type, "_fields");
    }
    void nextField(const RtlFieldInfo * field)
    {
        if (pushback)
            elem.setown(pushback.getClear());
        else if (field && named) // If it's named tuple, expect to always resolve fields by name, not position
        {
            elem.setown(PyObject_GetAttrString(parent, field->name));
        }
        else if (iter)
            elem.setown(PyIter_Next(iter));
        else
            elem = NULL;
        checkPythonError();
    }
    OwnedPyObject iter;
    OwnedPyObject pushback;
    OwnedPyObject elem;
    OwnedPyObject parent;
    bool named;
    PointerArray iterStack;
    PointerArray parentStack;
    BoolArray namedStack;
};

static size32_t getRowResult(PyObject *result, ARowBuilder &builder)
{
    PythonRowBuilder pyRowBuilder(result);
    const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
    assertex(typeInfo);
    RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
    return typeInfo->build(builder, 0, &dummyField, pyRowBuilder);
}

// A PythonNamedTupleBuilder object is used to construct a Python named tuple from an ECL row

class PythonNamedTupleBuilder : public CInterfaceOf<IFieldProcessor>
{
public:
    PythonNamedTupleBuilder(PythonThreadContext *_sharedCtx, const RtlFieldInfo *_outerRow)
    : outerRow(_outerRow), sharedCtx(_sharedCtx)
    {
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        addArg(PyUnicode_FromStringAndSize(value, len));
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        addArg(PyBool_FromLong(value ? 1 : 0));
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        addArg(PyByteArray_FromStringAndSize((const char *) value, len));
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        addArg(PyLong_FromLongLong(value));
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        addArg(PyLong_FromUnsignedLongLong(value));
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        addArg(PyFloat_FromDouble(value));
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setDecimal(digits, precision, value);
        addArg(PyFloat_FromDouble(val.getReal()));
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        val.setUDecimal(digits, precision, value);
        addArg(PyFloat_FromDouble(val.getReal()));
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        // You don't really know what size Py_UNICODE is (varies from system to system), so go via utf8
        unsigned unicodeChars;
        rtlDataAttr unicode;
        rtlUnicodeToUtf8X(unicodeChars, unicode.refstr(), len, value);
        processUtf8(unicodeChars, unicode.getstr(), field);
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
        size32_t sizeBytes = rtlUtf8Size(len, value);
        PyObject *vval = PyUnicode_FromStringAndSize(value, sizeBytes);   // NOTE - requires size in bytes not chars
        checkPythonError();
        addArg(vval);
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        push();
        if (isAll)
            rtlFail(0, "pyembed: ALL sets are not supported");
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        push();
        return true;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
            push();
        return true;
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
        {
            args.setown(getTuple(field->type));
            pop();
        }
    }
    PyObject *getTuple(const RtlTypeInfo *type)
    {
        OwnedPyObject mynamedtupletype = sharedCtx ? sharedCtx->getNamedTupleType(type) : globalState.getNamedTupleType(type);
#ifdef USE_CUSTOM_NAMEDTUPLES
        OwnedPyObject argsTuple = PyTuple_New(1);
        Py_INCREF(args);
        PyTuple_SET_ITEM((PyTupleObject *) argsTuple.get(), 0, args);
#else
        OwnedPyObject argsTuple = PyList_AsTuple(args);
#endif
        OwnedPyObject mynamedtuple = PyObject_CallObject(mynamedtupletype, argsTuple);  // Creates a namedtuple from the supplied tuple
        checkPythonError();
        return mynamedtuple.getClear();
    }
protected:
    void push()
    {
        stack.append(args.getClear());
        args.setown(PyList_New(0));
    }
    void pop()
    {
        OwnedPyObject arg = args.getClear();
        args.setown((PyObject *) stack.popGet());
        addArg(arg.getClear());
    }
    void addArg(PyObject *arg)
    {
        if (!args)
        {
            args.setown(PyList_New(0));
        }
        PyList_Append(args, arg);
        Py_DECREF(arg);
    }
    OwnedPyObject args;
    PointerArray stack;
    const RtlFieldInfo *outerRow;
    PythonThreadContext *sharedCtx;
};

//----------------------------------------------------------------------

// GILBlock ensures the we hold the Python "Global interpreter lock" for the appropriate duration

class GILBlock
{
public:
    GILBlock(PyThreadState * &_state) : state(_state)
    {
        PyEval_RestoreThread(state);
    }
    ~GILBlock()
    {
        state = PyEval_SaveThread();
    }
private:
    PyThreadState * &state;
};


// GILUnblock ensures the we release the Python "Global interpreter lock" for the appropriate duration

class GILUnblock
{
public:
    GILUnblock()
    {
        state = PyEval_SaveThread();
    }
    ~GILUnblock()
    {
        PyEval_RestoreThread(state);
    }
private:
    PyThreadState *state;
};

//----------------------------------------------------------------------

// Wrap an IRowStream into a Python generator

struct ECLDatasetIterator
{
    PyObject_HEAD;
    const RtlTypeInfo *typeInfo;  // Not linked (or linkable)
    IRowStream * val;  // Linked
};

PyObject* ECLDatasetIterator_iter(PyObject *self)
{
      Py_INCREF(self);
      return self;
}

void ECLDatasetIterator_dealloc(PyObject *self)
{
    ECLDatasetIterator *p = (ECLDatasetIterator *)self;
    if (p->val)
    {
        GILUnblock b;
        p->val->stop();
        ::Release(p->val);
        p->val = NULL;
    }
    self->ob_type->tp_free(self);
}

PyObject* ECLDatasetIterator_iternext(PyObject *self)
{
    ECLDatasetIterator *p = (ECLDatasetIterator *)self;
    roxiemem::OwnedConstRoxieRow nextRow;
    if (p->val)
    {
        GILUnblock b;
        nextRow.setown(p->val->ungroupedNextRow());
        if (!nextRow)
        {
            p->val->stop();
            ::Release(p->val);
            p->val = NULL;
        }
    }
    if (p->val)
    {
        RtlFieldStrInfo dummyField("<row>", NULL, p->typeInfo);
        PythonNamedTupleBuilder tupleBuilder(NULL, &dummyField);
        const byte *brow = (const byte *) nextRow.get();
        p->typeInfo->process(brow, brow, &dummyField, tupleBuilder);
        return tupleBuilder.getTuple(p->typeInfo);
    }
    else
    {
        // If we get here, it's EOF
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

static PyTypeObject ECLDatasetIteratorType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "ECLDatasetIterator._MyIter",      /*tp_name*/
    sizeof(ECLDatasetIterator),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    ECLDatasetIterator_dealloc,        /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,  /* tp_flags: tell python to use tp_iter and tp_iternext fields. */
    "ECL dataset iterator object.",           /* tp_doc */
    0,  /* tp_traverse */
    0,  /* tp_clear */
    0,  /* tp_richcompare */
    0,  /* tp_weaklistoffset */
    ECLDatasetIterator_iter,  /* tp_iter: __iter__() method */
    ECLDatasetIterator_iternext  /* tp_iternext: next() method */
};

static PyObject *createECLDatasetIterator(const RtlTypeInfo *_typeInfo, IRowStream * _val)
{
    ECLDatasetIteratorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ECLDatasetIteratorType) < 0)  return NULL;

    ECLDatasetIterator *p = PyObject_New(ECLDatasetIterator, &ECLDatasetIteratorType);
    if (!p)
    {
        checkPythonError();
        rtlFail(0, "pyembed: failed to create dataset iterator");
    }
    p->typeInfo = _typeInfo;
    p->val = _val;
    return (PyObject *)p;
}

//-----------------------------------------------------


void Python3xGlobalState::unregister(const char *key)
{
    checkThreadContext();
    GILBlock b(threadContext->threadState);
    globalState.releaseNamedScope(key);
}

void Python3xGlobalState::removePath(const char *path)
{
    checkThreadContext();
    GILBlock b(threadContext->threadState);
    // Remove a manifest file from the Python path, and remove from sys.modules any modules loaded from that location
    PyObject *sysPath = PySys_GetObject((char *) "path");
    if (sysPath)
    {
        OwnedPyObject newPath = PyUnicode_FromString(path);
        Py_ssize_t found = PySequence_Index(sysPath, newPath);
        if (found != (Py_ssize_t)-1)
        {
            PySequence_DelItem(sysPath, found);
            checkPythonError();
        }
        else
            PyErr_Clear();
        PyObject *sysModules = PySys_GetObject((char *) "modules");
        checkPythonError();
        OwnedPyObject values = PyDict_Values(sysModules);
        checkPythonError();
        Py_ssize_t len = PyList_Size(values);
        size_t pathLen = strlen(path);
        for (Py_ssize_t idx = 0; idx < len; idx++)
        {
            PyObject *module = PyList_GetItem(values, idx);
            if (PyObject_HasAttrString(module, "__file__"))
            {
                OwnedPyObject file = PyObject_GetAttrString(module, "__file__");
                if (file && PyUnicode_Check(file))
                {
                    const char *fileName = PyUnicode_AsUTF8AndSize(file, NULL);
                    if (strncmp(fileName, path, pathLen)==0)
                    {
                        OwnedPyObject modname = PyObject_GetAttrString(module, "__name__");
                        DBGLOG("Unloading module %s", fileName);
                        PyDict_DelItem(sysModules, modname);
                    }
                }
            }
            checkPythonError();
        }
    }
}

// A Python function that returns a dataset will return a PythonRowStream object that can be
// interrogated to return each row of the result in turn

class PythonRowStream : public CInterfaceOf<IRowStream>
{
public:
    PythonRowStream(PyObject *result, IEngineRowAllocator *_resultAllocator)
    : resultIterator(NULL)
    {
        // NOTE - the caller should already have the GIL lock before creating me
        if (!result || result == Py_None)
            typeError("list or generator", NULL);
        resultIterator.setown(PyObject_GetIter(result));   // We allow anything that is iterable to be returned for a row stream
        checkPythonError();
        resultAllocator.set(_resultAllocator);
    }
    ~PythonRowStream()
    {
        if (resultIterator)
        {
            checkThreadContext();
            GILBlock b(threadContext->threadState);
            resultIterator.clear();
        }
    }
    virtual const void *nextRow()
    {
        checkThreadContext();
        GILBlock b(threadContext->threadState);
        if (!resultIterator)
            return NULL;
        OwnedPyObject row = PyIter_Next(resultIterator);
        checkPythonError();
        if (!row)
            return NULL;
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        size32_t len = py3embed::getRowResult(row, rowBuilder);
        checkPythonError();
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        checkThreadContext();
        GILBlock b(threadContext->threadState);
        resultAllocator.clear();
        resultIterator.clear();
    }

protected:
    Linked<IEngineRowAllocator> resultAllocator;
    OwnedPyObject resultIterator;
};

// Each call to a Python function will use a new Python3xEmbedFunctionContext object
// This takes care of ensuring that the Python GIL is locked while we are executing python code,
// and released when we are not

class Python3xEmbedContextBase : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    Python3xEmbedContextBase(PythonThreadContext *_sharedCtx, ICodeContext *_codeCtx)
    : sharedCtx(_sharedCtx), codeCtx(_codeCtx)
    {
        PyEval_RestoreThread(sharedCtx->threadState);
    }

    virtual void setActivityOptions(const IThorActivityContext *ctx)
    {
        OwnedPyObject mynamedtupletype = globalState.getActivityContextTupleType();
        OwnedPyObject args = PyTuple_New(5);
        OwnedPyObject isLocal;
        isLocal.set(ctx->isLocal() ? Py_True : Py_False);
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), 0, isLocal.getClear());
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), 1, PyLong_FromLong(ctx->numSlaves()));
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), 2, PyLong_FromLong(ctx->numStrands()));
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), 3, PyLong_FromLong(ctx->querySlave()));
        PyTuple_SET_ITEM((PyTupleObject *) args.get(), 4, PyLong_FromLong(ctx->queryStrand()));
#ifdef USE_CUSTOM_NAMEDTUPLES
        OwnedPyObject argsTuple = PyTuple_New(1);
        PyTuple_SET_ITEM((PyTupleObject *) argsTuple.get(), 0, args.getClear());
        OwnedPyObject activityTuple = PyObject_CallObject(mynamedtupletype, argsTuple);  // Creates a namedtuple from the supplied tuple
#else
        OwnedPyObject activityTuple = PyObject_CallObject(mynamedtupletype, args);  // Creates a namedtuple from the supplied tuple
#endif
        checkPythonError();
        PyDict_SetItemString(locals, "__activity__", activityTuple.getClear());
        checkPythonError();
    }

    void setScopes(ICodeContext *codeCtx, const char *_options)
    {
        locals.setown(PyDict_New());
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
            globals.setown(globalState.getNamedScope(scopeKey, isNew));
            if (isNew && engine)
                engine->onTermination(Python3xGlobalState::unregister, scopeKey.str(), wuidScope);
        }
        else
            globals.setown(PyDict_New());
        PyDict_SetItemString(globals, "__builtins__",  PyEval_GetBuiltins());  // required for import to work
    }
    ~Python3xEmbedContextBase()
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
        return py3embed::getBooleanResult(NULL, result);
    }
    virtual void getDataResult(size32_t &__chars, void * &__result)
    {
        py3embed::getDataResult(NULL, result, __chars, __result);
    }
    virtual double getRealResult()
    {
        return py3embed::getRealResult(NULL, result);
    }
    virtual __int64 getSignedResult()
    {
        return py3embed::getSignedResult(NULL, result);
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return py3embed::getUnsignedResult(NULL, result);
    }
    virtual void getStringResult(size32_t &__chars, char * &__result)
    {
        py3embed::getStringResult(NULL, result, __chars, __result);
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        py3embed::getUTF8Result(NULL, result, __chars, __result);
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        py3embed::getUnicodeResult(NULL, result, __chars, __result);
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        py3embed::getSetResult(result, __isAllResult, __resultBytes, __result, elemType, elemSize);
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new PythonRowStream(result, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        size32_t len = py3embed::getRowResult(result, rowBuilder);
        return (byte *) rowBuilder.finalizeRowClear(len);
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        return py3embed::getRowResult(result, builder);
    }
    virtual void bindBooleanParam(const char *name, bool val)
    {
        addArg(name, PyBool_FromLong(val ? 1 : 0));
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        addArg(name, PyByteArray_FromStringAndSize((const char *) val, len));
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        addArg(name, PyFloat_FromDouble((double) val));
    }
    virtual void bindRealParam(const char *name, double val)
    {
        addArg(name, PyFloat_FromDouble(val));
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        addArg(name, PyLong_FromLongLong(val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        addArg(name, PyLong_FromLongLong(val));
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        addArg(name, PyLong_FromUnsignedLongLong(val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        addArg(name, PyLong_FromUnsignedLongLong(val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        addArg(name, PyUnicode_Decode(val, len, ASCII_LIKE_CODEPAGE, "ignore"));
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        addArg(name, PyUnicode_FromString(val));
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
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
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
                thisElem.setown(PyUnicode_FromStringAndSize((const char *) inData, numChars));
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
                thisElem.setown(PyUnicode_FromStringAndSize((const char *) inData, thisSize));
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
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val) override
    {
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        PythonNamedTupleBuilder tupleBuilder(sharedCtx, &dummyField);
        typeInfo->process(val, val, &dummyField, tupleBuilder); // Creates a tuple from the incoming ECL row
        addArg(name, tupleBuilder.getTuple(typeInfo));
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        addArg(name, createECLDatasetIterator(metaVal.queryTypeInfo(), LINK(val)));
    }
protected:
    virtual void addArg(const char *name, PyObject *arg) = 0;

    PythonThreadContext *sharedCtx = nullptr;
    ICodeContext *codeCtx = nullptr;
    OwnedPyObject locals;
    OwnedPyObject globals;
    OwnedPyObject result;
    OwnedPyObject script;
};

class Python3xEmbedScriptContext : public Python3xEmbedContextBase
{
public:
    Python3xEmbedScriptContext(PythonThreadContext *_sharedCtx, ICodeContext *_codeCtx)
    : Python3xEmbedContextBase(_sharedCtx, _codeCtx)
    {
    }
    ~Python3xEmbedScriptContext()
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

    virtual void importFunction(size32_t lenChars, const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        script.setown(sharedCtx->compileEmbeddedScript(codeCtx, lenChars, utf, argstring));
    }
    virtual void loadCompiledScript(size32_t chars, const void *_script) override
    {
        throwUnexpected();
    }
    virtual void enter() override {}
    virtual void reenter(ICodeContext *codeCtx) override {}
    virtual void exit() override {}
    virtual void setActivityOptions(const IThorActivityContext *ctx) override
    {
        Python3xEmbedContextBase::setActivityOptions(ctx);
        argstring.append("__activity__");
    }


    virtual void callFunction()
    {
        result.setown(PyEval_EvalCode(script.get(), globals, locals));
        checkPythonError();
        if (!result || result == Py_None)
            result.set(PyDict_GetItemString(globals, "__result__"));
        if (!result || result == Py_None)
            result.set(PyDict_GetItemString(locals, "__result__"));
    }
    void setargs(const char *args)
    {
        argstring.set(args);
    }
protected:
    virtual void addArg(const char *name, PyObject *arg)
    {
        if (!arg)
            return;
        if (argstring.length())
            argstring.append(',');
        argstring.append(name);
        if (script)
            PyDict_SetItemString(globals, name, arg);  // Back compatibility - if compiler did not recognize the prebind flag, we need to use globals
        else
            PyDict_SetItemString(locals, name, arg);
        Py_DECREF(arg);
        checkPythonError();
    }
    StringBuffer argstring;
};

class Python3xEmbedImportContext : public Python3xEmbedContextBase
{
public:
    Python3xEmbedImportContext(PythonThreadContext *_sharedCtx, ICodeContext *_codeCtx)
    : Python3xEmbedContextBase(_sharedCtx, _codeCtx)
    {
        argcount = 0;
    }
    ~Python3xEmbedImportContext()
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

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        script.setown(sharedCtx->importFunction(codeCtx, lenChars, utf));
    }
    virtual void compileEmbeddedScript(size32_t len, const char *text)
    {
        throwUnexpected();
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

class Python3xEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
    {
        return createFunctionContextEx(nullptr, nullptr, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
    {
        checkThreadContext();
        Owned<Python3xEmbedContextBase> ret;
        if (flags & EFimport)
            ret.setown(new Python3xEmbedImportContext(threadContext, ctx));
        else
            ret.setown(new Python3xEmbedScriptContext(threadContext, ctx));
        ret->setScopes(ctx, options);
        if (activityCtx)
            ret->setActivityOptions(activityCtx);
        return ret.getClear();
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
    {
        throwUnexpected();
    }
};

extern DECL_EXPORT IEmbedContext* getEmbedContext()
{
    return new Python3xEmbedContext;
}

extern DECL_EXPORT void syntaxCheck(size32_t & __lenResult, char * & __result, const char *funcname, size32_t charsBody, const char * body, const char *argNames, const char *compilerOptions, const char *persistOptions)
{
    StringBuffer result;
    if (globalState.isInitialized())
    {
        // NOTE - compilation of a script does not actually resolve imports - so the fact that the manifest is not on the path does not matter
        // This does mean that many errors cannot be caught until runtime, but that's Python for you...
        try
        {
            checkThreadContext();
            Owned<Python3xEmbedScriptContext> ctx = new Python3xEmbedScriptContext(threadContext, nullptr);
            ctx->setargs(argNames);
            ctx->compileEmbeddedScript(charsBody, body);
        }
        catch (IException *E)
        {
            StringBuffer msg;
            result.append(E->errorMessage(msg));
            E->Release();
        }
    }
    __lenResult = result.length();
    __result = result.detach();
}

} // namespace
