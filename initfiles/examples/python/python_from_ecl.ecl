/* Example of calling Python from ECL code via embedded C++
*
* This example uses the following code in python_cat.py:
*
* def cat(a, b):
*   return a + b
*
* To compile this ECL example, you need to link the Python libraries:
*
* eclcc python_from_ecl.ecl -Wc,-I/usr/include/python2.7/ -Wl,-lpython2.7 -target=hthor
*
* To run it, ensure that PYTHONPATH is set such that python_cat.py can be located
*/

// Embedded C++ that makes a JNI call

string cat(varstring a, varstring b) := BEGINC++

// This section of the code should probably move to a plugin, or somesuch

#include <Python.h>
#include <assert.h>

static pthread_once_t python_resolve_flag = PTHREAD_ONCE_INIT;  /* Ensures called just once */
static PyObject *pName, *pModule, *pFunc_cat;
static bool pythonInitialized = false;

static void resolvePythonFunctions()
{
    /* Do all the function resolution just the once... */
    PyObject *pDict;

    // Initialize the Python Interpreter
    Py_Initialize();
    PyEval_InitThreads();
    pythonInitialized = true;

    // Build the name object
    pName = PyString_FromString("python_cat");

    // Load the module object
    pModule = PyImport_Import(pName);
    if (pModule == NULL)
    {
        PyErr_Print();
    }
    else
    {
        // pDict is a borrowed reference
        pDict = PyModule_GetDict(pModule);
        // pFunc_cat is also a borrowed reference
        pFunc_cat = PyDict_GetItemString(pDict, "cat");
        if (!pFunc_cat || !PyCallable_Check(pFunc_cat))
        {
            PyErr_Print();
            pFunc_cat = NULL;
        }
    }
    PyEval_ReleaseLock();
}

static void finishPython()
{
    // Clean up
    if (pModule)
        Py_DECREF(pModule);
    if (pName)
        Py_DECREF(pName);
    // Finish the Python Interpreter
    if (pythonInitialized)
        Py_Finalize();
}

static void checkPythonError()
{
    PyObject* err = PyErr_Occurred();
    if (err)
    {
        PyErr_Print();
        rtlFail(0, "Unexpected failure"); // MORE - should probably get some info out of PyError rather than just printing it
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

//--------------------------------------------------------

#body

// extern  void user1(size32_t & __lenResult,char * & __result,const char * a,const char * b) {
    pthread_once(&python_resolve_flag, resolvePythonFunctions);
    if (!pFunc_cat)
       rtlFail(0, "Could not resolve python functions");
    GILstateWrapper gstate; // Ensure that we play nice with Python threads

    OwnedPyObject pArgs = Py_BuildValue("s,s", a, b);
    checkPythonError();
    OwnedPyObject pResult = PyObject_CallObject(pFunc_cat, pArgs);
    checkPythonError();

    __lenResult = PyString_Size(pResult);
    const char * chars =  PyString_AsString(pResult);
    __result = new char(__lenResult);
    memcpy(__result, chars, __lenResult);
// }
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
