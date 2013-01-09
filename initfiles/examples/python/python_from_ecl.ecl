/* Example of calling Python from ECL code via embedded C++
*
* This example uses the following code in python_cat.py:
*
* def cat(a, b):
*   return a + b
*
* Note that you may need to change the line that sets the Python sys.path, if you extend this
* code to use other python examples, or it you are running on a system other than a standard HPCC
* install on Linux
*
*/

#option('compileOptions', '-I/usr/include/python2.7/')
#option('linkOptions', '-lpython2.7')

// Embedded C++ that makes a call to a Python function


string cat(varstring a, varstring b) := BEGINC++

// This section of the code should probably move to a plugin, or somesuch

#include <Python.h>
#include <assert.h>
#include <pthread.h>

// #option library python2.7

static PyObject *pFunc_cat;

class PythonInitializer
{
    PyObject *pModule;
    PyThreadState *tstate;
    bool pythonInitialized;
public:
    PythonInitializer()
    {
        pModule = NULL;
        tstate = NULL;
        pythonInitialized = false;

        // Initialize the Python Interpreter
        Py_Initialize();
        PyEval_InitThreads();
        pythonInitialized = true;
        resolvePythonFunctions();
        tstate = PyEval_SaveThread();
    }
    ~PythonInitializer()
    {
        PyEval_RestoreThread(tstate);
        // Clean up
        if (pModule)
            Py_DECREF(pModule);
        // Finish the Python Interpreter
        if (pythonInitialized)
            Py_Finalize();
    }

    void resolvePythonFunctions()
    {
        PySys_SetPath("/opt/HPCCSystems/examples/python/");    // Set this to where you want to pick up python_cat.py from
        pModule = PyImport_ImportModule("python_cat");
        if (pModule == NULL)
        {
            PyErr_Print();
        }
        else
        {
            // pDict is a borrowed reference
            PyObject *pDict = PyModule_GetDict(pModule);
            // pFunc_cat is also a borrowed reference
            pFunc_cat = PyDict_GetItemString(pDict, "cat");
            if (!pFunc_cat || !PyCallable_Check(pFunc_cat))
            {
                PyErr_Print();
                pFunc_cat = NULL;
            }
        }
    }

};

PythonInitializer __initializer;


// Use class OwnedPyObject for any objects that are not 'borrowed references'
// so that the appropriate Py_DECREF call is made when the OwnedPyObject goes
// out of scope, even if the function returns prematurely (such as via an exception).
// In particular, checkPythonError is a lot easier to call safely if this is used.

class OwnedPyObject
{
    PyObject *ptr;
public:
    inline OwnedPyObject(PyObject *_ptr) : ptr(_ptr) {}
    inline ~OwnedPyObject()                { if (ptr) Py_DECREF(ptr); }
    inline PyObject * operator -> () const { return ptr; }
    inline operator PyObject *() const     { return ptr; }
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

//--------------------------------------------------------

#body

// extern  void user1(size32_t & __lenResult,char * & __result,const char * a,const char * b) {
{
    if (!pFunc_cat)
       rtlFail(0, "Could not resolve python functions");
    GILstateWrapper gstate;
    PyObject *pArgs = Py_BuildValue("s,s", a, b);
    checkPythonError();
    PyObject *pResult = PyObject_CallObject(pFunc_cat, pArgs);
    checkPythonError();
     __lenResult = PyString_Size(pResult);
    const char * chars =  PyString_AsString(pResult);
    checkPythonError();
    __result = new char(__lenResult);
    memcpy(__result, chars, __lenResult);
}
ENDC++;

//--------------------------------------------------------

// ECL code - an input dataset with 2 records, each containing 2 strings

inrec := RECORD
           string f1;
           string f2;
         END;
infile1 := DATASET([{'a', 'b'}, {'c', 'd'}], inrec);
infile2 := DATASET([{'e', 'f'}, {'g', 'h'}], inrec);

// Output record has just one string, filled in from the result of the java function
outrec := RECORD
            string c;
          END;

outrec t(inrec L) := TRANSFORM
  SELF.c := cat(L.f1, L.f2)  // Calls Python function
END;

outfile := project(infile1, t(LEFT))+project(infile2, t(LEFT));  // threaded concat operation

outfile;
