/* Example of calling Python from ECL code via embedded C++
*
* This example evalues a python expression within an ECL transform
* Because the python will be compiled every timr the expression is evaluated, it
* is likely to be less efficient than calling external python code as in the example
* python_from_ecl
*
*/

#option('compileOptions', '-I/usr/include/python2.7/')
#option('linkOptions', '-lpython2.7')

// Embedded C++ that makes a call to a Python function


string python(varstring expr) := BEGINC++

// This section of the code should probably move to a plugin, or somesuch

#include <Python.h>
#include <assert.h>
#include <pthread.h>

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
    inline PyObject * get() const          { return ptr; }
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

// extern  void user1(size32_t & __lenResult,char * & __result, const char * expr) {
{
    GILstateWrapper gstate;
    checkPythonError();
    OwnedPyObject locals = PyDict_New ();
    OwnedPyObject globals = PyDict_New ();
    OwnedPyObject pResult = PyRun_String(expr, Py_eval_input, locals, globals);
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
  SELF.c := python('"' + L.f1 + '" + "' + L.f2 + '"')  // Evaluates python expression to concatenate strings
END;

outfile := project(infile1, t(LEFT))+project(infile2, t(LEFT));  // threaded concat operation

outfile;
