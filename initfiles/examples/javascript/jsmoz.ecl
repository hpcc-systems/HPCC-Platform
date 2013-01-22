/* Example of calling JavaScript (Mozilla SpiderMonkey) from ECL code via embedded C++
*
* This example evalues a JavaScript expression within an ECL transform
*
*/

#option('compileOptions', '-I/usr/include/js/')
#option('linkOptions', '-lmozjs185')

// Embedded C++ that makes a call to a JavaScript function

string jseval(varstring script, varstring a, varstring b) := BEGINC++

// This section of the code should probably move to a plugin, or somesuch

/* Include the JSAPI header file to get access to SpiderMonkey. */
#include "jsapi.h"
#include <pthread.h>

/* The class of the global object. */
static JSClass global_class = {
    "global", JSCLASS_GLOBAL_FLAGS,
    JS_PropertyStub, JS_PropertyStub, JS_PropertyStub, JS_StrictPropertyStub,
    JS_EnumerateStub, JS_ResolveStub, JS_ConvertStub, JS_FinalizeStub,
    JSCLASS_NO_OPTIONAL_MEMBERS
};

class MozJsContext
{
    JSRuntime *rt;
    JSContext *cx;
    JSObject  *global;

public:
    MozJsContext()
    {
        // Initialize to NULL so that initFailed can be used
        rt = NULL;
        cx = NULL;
        global = NULL;

        // We use a separate runtime each time - this may be a bad idea
        rt = JS_NewRuntime(8 * 1024 * 1024);
        if (!rt)
            initFailed("Could not create JavaScript runtime");

        // We need a context per thread - we COULD share between JS calls on a thread (using TLS, for example)
        cx = JS_NewContext(rt, 8192);
        if (cx == NULL)
            initFailed("Could not create JavaScript context");
        JS_SetOptions(cx, JSOPTION_VAROBJFIX | JSOPTION_JIT | JSOPTION_METHODJIT);
        JS_SetVersion(cx, JSVERSION_LATEST);
        JS_SetErrorReporter(cx, reportError);
        /*
         * Create the global object in a new compartment.
         * You always need a global object per context.
         */
        global = JS_NewCompartmentAndGlobalObject(cx, &global_class, NULL);
        if (global == NULL)
            initFailed("Could not create JavaScript global object");
        /*
         * Populate the global object with the standard JavaScript
         * function and object classes, such as Object, Array, Date.
         */
        if (!JS_InitStandardClasses(cx, global))
            initFailed("Could not populate JavaScript global object");
        JS_BeginRequest(cx);  // Probably not really necessary with a separate runtime per instance, but will be as soon as we change that.
    }
    ~MozJsContext()
    {
        if (global)
            JS_EndRequest(cx);
        if (cx)
            JS_DestroyContext(cx);
        if (rt)
            JS_DestroyRuntime(rt);
    }
    inline operator JSContext *() const { return cx; }
    inline JSObject *queryGlobal() const { return global; }
private:
    void cleanup()
    {
        if (cx)
            JS_DestroyContext(cx);
        if (rt)
            JS_DestroyRuntime(rt);
    }
    void initFailed(const char *why)
    {
        cleanup();
        rtlFail(0, why);
    }
    static void reportError(JSContext *cx, const char *message, JSErrorReport *report)
    {
        // MORE - need to think about what is appropriate here!
        fprintf(stderr, "%s:%u:%s\n",
                 report->filename ? report->filename : "<no filename=\"filename\">",
                 (unsigned int) report->lineno,
                 message);
    }
};

//#body

// extern  void user1(size32_t & __lenResult,char * & __result, const char * script, const char *a, const char *b) {
{
    MozJsContext c;
    JSString * aa = JS_NewStringCopyZ(c, a);
    JSString * bb = JS_NewStringCopyZ(c, b);
    jsval rval;
    JS_DefineProperty(c, c.queryGlobal(), "a", STRING_TO_JSVAL(aa), NULL, NULL, JSPROP_READONLY);
    JS_DefineProperty(c, c.queryGlobal(), "b", STRING_TO_JSVAL(bb), NULL, NULL, JSPROP_READONLY);
    JSBool ok = JS_EvaluateScript(c, c.queryGlobal(), script, strlen(script), __FILE__, __LINE__, &rval);
    if (rval == NULL | rval == JS_FALSE)
        rtlFail(0, "Error in JavaScript evaluation");
    JSString *str = JS_ValueToString(c, rval);
    const char * chars = JS_EncodeString(c, str);
    __lenResult = strlen(chars);
    __result = (char *) rtlMalloc(__lenResult);
    memcpy(__result, chars, __lenResult);
}
ENDC++;

//--------------------------------------------------------

// ECL code - an input dataset with 2 records, each containing 2 strings
// Note that it uses the threaded concat operation, to test that multi-threaded access to the JS engine works

inrec := RECORD
           string f1;
           string f2;
         END;
infile1 := DATASET([{'a', 'b'}, {'c', 'd'}], inrec);
infile2 := DATASET([{'e', 'f'}, {'g', 'h'}], inrec);

// Output record has just one string, filled in from the result of the JavaScript function
outrec := RECORD
            string c;
          END;

outrec t(inrec L) := TRANSFORM
  SELF.c := jseval('a + b', L.f1, L.f2)  // Evaluates JavaScript expression to concatenate strings
END;

outfile := project(infile1, t(LEFT))+project(infile2, t(LEFT));  // threaded concat operation

outfile;
