#include "secure-enclave.hpp"

#include "jexcept.hpp"
#include "hqlplugins.hpp"

static const char *compatibleVersions[] = {
    "WASM Embed Helper 1.0.0",
    NULL};

static const char *version = "WASM Embed Helper 1.0.0";

extern "C" DECL_EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx *pbx = (ECLPluginDefinitionBlockEx *)pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = version;
    pb->moduleName = "wasm";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "WASM Embed Helper";
    return true;
}

namespace wasmLanguageHelper
{
    class WasmEmbedContext : public CInterfaceOf<IEmbedContext>
    {
    public:
        virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
        {
            return createFunctionContextEx(nullptr, nullptr, flags, options);
        }

        virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext *codeCtx, const IThorActivityContext *activityContext, unsigned flags, const char *options) override
        {
            return createISecureEnclave(codeCtx);
        }

        virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
        {
            throwUnexpected();
            return nullptr;
        }
    } theEmbedContext;

    extern DECL_EXPORT IEmbedContext *getEmbedContext()
    {
        return LINK(&theEmbedContext);
    }

    extern DECL_EXPORT void syntaxCheck(size32_t &__lenResult, char *&__result, const char *funcname, size32_t charsBody, const char *body, const char *argNames, const char *compilerOptions, const char *persistOptions)
    {
        StringBuffer result;
        //  MORE - ::syntaxCheck(__lenResult, __result, funcname, charsBody, body, argNames, compilerOptions, persistOptions);
        __lenResult = result.length();
        __result = result.detach();
    }

} // namespace
