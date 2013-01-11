/* Example of calling JavaScript (V8 engine) from ECL code via embedded C++
*
* This example evalues a JS expression within an ECL transform
*
*/

#option('linkOptions', '-lv8')

// Embedded C++ that makes a evaluates the script passed to it

string jseval(varstring script, varstring a, varstring b) := BEGINC++

// This section of the code should probably move to a plugin, or somesuch

/* Include the JSAPI header file to get access to SpiderMonkey. */
#include "v8.h"

#body

using namespace v8;

// extern  void user1(size32_t & __lenResult,char * & __result, const char * script, const char *a, const char *b) {
{
    Isolate* isolate = Isolate::New();
    {
        v8::Isolate::Scope iscope(isolate);
        v8::V8::Initialize();
        // Create a stack-allocated handle scope.
        HandleScope handle_scope;
        Persistent<Context> context = Context::New();
        Context::Scope context_scope(context);

        // Bind the parameters into the context
        context->Global()->Set(String::New("a"), String::New(a));
        context->Global()->Set(String::New("b"), String::New(b));

        // Create a string containing the JavaScript source code.
        Handle<String> source = String::New(script);

        // Compile the source code.
        Handle<Script> script = Script::Compile(source);

        // Run the script to get the result.
        Handle<Value> result = script->Run();

        // Dispose the persistent context.
        context.Dispose();

        // Convert the result to an ASCII string and return it.
        String::AsciiValue ascii(result);
        const char *chars= *ascii;
        __lenResult = strlen(chars);
        __result = new char(__lenResult);
        memcpy(__result, chars, __lenResult);
    }
    isolate->Dispose();
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
  SELF.c := jseval('a + b', L.f1, L.f2)  // Evaluates python expression to concatenate strings
END;

outfile := project(infile1, t(LEFT))+project(infile2, t(LEFT));  // threaded concat operation

outfile;
