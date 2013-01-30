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
#include "RInside.h"
#include "eclrtl.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] =
{ "R Embed Helper 1.0.0", NULL };

static const char *version = "R Embed Helper 1.0.0";

static const char * EclDefinition =
    "EXPORT Language := SERVICE\n"
    "  boolean getEmbedContext():cpp,pure,namespace='Rembed',entrypoint='getEmbedContext',prototype='IEmbedContext* getEmbedContext()';\n"
    "  boolean syntaxCheck(const varstring src):cpp,pure,namespace='Rembed',entrypoint='syntaxCheck';\n"
    "  unload():cpp,pure,namespace='Rembed',entrypoint='unload';\n"
    "END;"
    "EXPORT getEmbedContext := Language.getEmbedContext;"
    "EXPORT syntaxCheck := Language.syntaxCheck;"
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
    pb->moduleName = "R";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_DLL_MODULE | PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "R Embed Helper";
    return true;
}

namespace Rembed
{

// Use a global object to ensure that the R instance is initialized only once

static class RGlobalState
{
public:
    RGlobalState()
    {
        const char *args[] = {"R", "--slave" };
        R = new RInside(2, args, true, false, false);

    }
    ~RGlobalState()
    {
        delete R;
    }
    RInside *R;
}* globalState = NULL;

static CriticalSection RCrit;  // R is single threaded - need to own this before making any call to R

static RGlobalState *queryGlobalState()
{
    CriticalBlock b(RCrit);
    if (!globalState)
        globalState = new RGlobalState;
    return globalState;
}

extern void unload()
{
    CriticalBlock b(RCrit);
    if (globalState)
        delete globalState;
    globalState = NULL;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    unload();
}

// Each call to a R function will use a new REmbedFunctionContext object
// This takes care of ensuring that the critsec is locked while we are executing R code,
// and released when we are not

class REmbedFunctionContext: public CInterfaceOf<IEmbedFunctionContext>
{
public:
    REmbedFunctionContext(RInside &_R, const char *options)
    : R(_R), block(RCrit), result(R_NilValue)
    {
    }
    ~REmbedFunctionContext()
    {
    }

    virtual bool getBooleanResult()
    {
        return ::Rcpp::as<bool>(result);
    }
    virtual double getRealResult()
    {
        return ::Rcpp::as<double>(result);
    }
    virtual __int64 getSignedResult()
    {
        return ::Rcpp::as<long int>(result); // Should really be long long, but RInside does not support that
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return ::Rcpp::as<unsigned long int>(result); // Should really be long long, but RInside does not support that
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        std::string str = ::Rcpp::as<std::string>(result);
        rtlStrToStrX(__len, __result, str.length(), str.data());
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        throw MakeStringException(MSGAUD_user, 0, "Rembed: %s: Unicode/UTF8 results not supported", func.c_str());
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        throw MakeStringException(MSGAUD_user, 0, "Rembed: %s: Unicode/UTF8 results not supported", func.c_str());
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        R[name] = val;
    }
    virtual void bindRealParam(const char *name, double val)
    {
        R[name] = val;
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        R[name] = (long int) val;
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        R[name] = (unsigned long int) val;
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        std::string s(val, len);
        R[name] = s;
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        R[name] = val;
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        UNIMPLEMENTED;
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        UNIMPLEMENTED;
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *utf)
    {
        func.assign(utf, rtlUtf8Size(lenChars, utf));
    }

    virtual void callFunction()
    {
        result = R.parseEval(func);
    }
private:
    RInside &R;
    RInside::Proxy result;
    std::string func;
    CriticalBlock block;
};

class REmbedContext: public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        return new REmbedFunctionContext(*queryGlobalState()->R, options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new REmbedContext;
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
