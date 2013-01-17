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
#include "v8.h"
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
    "V8 JavaScript Embed Helper 1.0.0",
    NULL };

static const char *version = "V8 JavaScript Embed Helper 1.0.0";
static const char * EclDefinition =
    "EXPORT Language := SERVICE\n"
    "  boolean getEmbedContext():cpp,pure,namespace='javascriptLanguageHelper',entrypoint='getEmbedContext',prototype='IEmbedContext* getEmbedContext()';\n"
    "  boolean syntaxCheck(const varstring src):cpp,pure,namespace='javascriptLanguageHelper',entrypoint='syntaxCheck';\n"
    "END;"
    "export getEmbedContext := Language.getEmbedContext;"
    "export syntaxCheck := Language.syntaxCheck;"
    "EXPORT boolean supportsImport := false;"
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
    pb->moduleName = "javascript";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_DLL_MODULE | PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "V8 JavaScript Embed Helper";
    return true;
}

namespace javascriptLanguageHelper {

class V8JavascriptEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    V8JavascriptEmbedFunctionContext()
    {
        isolate = v8::Isolate::New();
        isolate->Enter();
        context = v8::Context::New();
        context->Enter();
    }
    ~V8JavascriptEmbedFunctionContext()
    {
        script.Dispose();
        result.Dispose();
        context->Exit();
        context.Dispose();
        isolate->Exit();
        isolate->Dispose();
    }

    virtual void bindRealParam(const char *name, double val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Number::New(val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        // MORE - might need to check does not overflow 32 bits? Or store as a real?
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Integer::New(val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        // MORE - might need to check does not overflow 32 bits
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Integer::NewFromUnsigned(val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val, len));
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val));
    }

    virtual double getRealResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Number::Cast(*result)->Value();
    }
    virtual __int64 getSignedResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Integer::Cast(*result)->Value();
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return v8::Integer::Cast(*result)->Value();
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;   // May not strictly be needed?
        v8::String::AsciiValue ascii(result);
        const char *chars= *ascii;
        __len = strlen(chars);
        __result = (char *)rtlMalloc(__len);
        memcpy(__result, chars, __len);
    }

    virtual void compileEmbeddedScript(const char *text)
    {
        v8::HandleScope handle_scope;
        v8::Handle<v8::String> source = v8::String::New(text);
        v8::Handle<v8::Script> lscript = v8::Script::Compile(source);
        script = v8::Persistent<v8::Script>::New(lscript);
    }
    virtual void importFunction(const char *text)
    {
        UNIMPLEMENTED; // Not sure if meaningful for js
    }
    virtual void callFunction()
    {
        assertex (!script.IsEmpty());
        v8::HandleScope handle_scope;
        result = v8::Persistent<v8::Value>::New(script->Run());
    }

protected:
    v8::Isolate *isolate;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Script> script;
    v8::Persistent<v8::Value> result;
};

__thread V8JavascriptEmbedFunctionContext * theFunctionContext;  // We reuse per thread, for speed
__thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    ::Release(theFunctionContext);
    if (threadHookChain)
        (*threadHookChain)();
}

class V8JavascriptEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    V8JavascriptEmbedContext()
    {
        Link();  // Deliberately 'leak' in order to avoid freeing this global object
    }
    virtual IEmbedFunctionContext *createFunctionContext()
    {
        if (!theFunctionContext)
        {
            theFunctionContext = new V8JavascriptEmbedFunctionContext;
            threadHookChain = addThreadTermFunc(releaseContext);
        }
        return LINK(theFunctionContext);
    }
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
