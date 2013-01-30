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

    virtual void bindBooleanParam(const char *name, bool val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::Boolean::New(val));
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        v8::HandleScope handle_scope;
        v8::Local<v8::Array> array = v8::Array::New(len);
        const byte *vval = (const byte *) val;
        for (int i = 0; i < len; i++)
        {
            array->Set(v8::Number::New(i), v8::Integer::New(vval[i])); // feels horridly inefficient, but seems to be the expected approach
        }
        context->Global()->Set(v8::String::New(name), array);
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
        size32_t utfCharCount;
        char *utfText;
        rtlStrToUtf8X(utfCharCount, utfText, len, val);
        bindUTF8Param(name, utfCharCount, utfText);
        rtlFree(utfText);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val, rtlUtf8Size(chars, val)));
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        v8::HandleScope handle_scope;
        context->Global()->Set(v8::String::New(name), v8::String::New(val, chars));
    }

    virtual bool getBooleanResult()
    {
        assertex (!result.IsEmpty());
        v8::HandleScope handle_scope;
        return result->BooleanValue();
    }
    virtual void getDataResult(size32_t &__len, void * &__result)
    {
        assertex (!result.IsEmpty() && result->IsArray());
        v8::HandleScope handle_scope;
        v8::Handle<v8::Array> array = v8::Handle<v8::Array>::Cast(result);
        __len = array->Length();
        __result = rtlMalloc(__len);
        byte *bresult = (byte *) __result;
        for (size32_t i = 0; i < __len; i++)
        {
            bresult[i] = v8::Integer::Cast(*array->Get(i))->Value(); // feels horridly inefficient, but seems to be the expected approach
        }
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
    virtual void getStringResult(size32_t &__chars, char * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::AsciiValue ascii(result);
        rtlStrToStrX(__chars, __result, ascii.length(), *ascii);
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::Utf8Value utf8(result);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUtf8X(__chars, __result, numchars, *utf8);
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        assertex (!result.IsEmpty() && result->IsString());
        v8::HandleScope handle_scope;
        v8::String::Utf8Value utf8(result);
        unsigned numchars = rtlUtf8Length(utf8.length(), *utf8);
        rtlUtf8ToUnicodeX(__chars, __result, numchars, *utf8);
    }

    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        v8::HandleScope handle_scope;
        v8::Handle<v8::String> source = v8::String::New(utf, rtlUtf8Size(lenChars, utf));
        v8::Handle<v8::Script> lscript = v8::Script::Compile(source);
        script = v8::Persistent<v8::Script>::New(lscript);
    }
    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        UNIMPLEMENTED; // Not sure if meaningful for js
    }
    virtual void callFunction()
    {
        assertex (!script.IsEmpty());
        v8::HandleScope handle_scope;
        v8::TryCatch tryCatch;
        result = v8::Persistent<v8::Value>::New(script->Run());
        v8::Handle<v8::Value> exception = tryCatch.Exception();
        if (!exception.IsEmpty())
        {
            v8::String::AsciiValue msg(exception);
            throw MakeStringException(MSGAUD_user, 0, "v8embed: %s", *msg);
        }
    }

protected:
    v8::Isolate *isolate;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Script> script;
    v8::Persistent<v8::Value> result;
};

static __thread V8JavascriptEmbedFunctionContext * theFunctionContext;  // We reuse per thread, for speed
static __thread ThreadTermFunc threadHookChain;

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
    }
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        assertex(!isImport);
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
