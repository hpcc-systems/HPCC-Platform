#include "platform.h"
#include "eclrtl.hpp"

#ifdef SECUREENCLAVE_EXPORTS
 #define SECUREENCLAVE_API DECL_EXPORT
#else
 #define SECUREENCLAVE_API DECL_IMPORT
#endif

#include <memory>

interface IWasmEmbedCallback
{
    virtual inline void DBGLOG(char const *format, ...) __attribute__((format(printf, 2, 3))) = 0;
    virtual void *rtlMalloc(size32_t size) = 0;

    virtual const char *resolveManifestPath(const char *leafName) = 0;
};

interface ISecureEnclave : extends IEmbedFunctionContext
{
    virtual ~ISecureEnclave() = default;
};

SECUREENCLAVE_API void init(std::shared_ptr<IWasmEmbedCallback> embedContext);
SECUREENCLAVE_API void kill();
SECUREENCLAVE_API std::unique_ptr<ISecureEnclave> createISecureEnclave();
SECUREENCLAVE_API void syntaxCheck(size32_t &__lenResult, char *&__result, const char *funcname, size32_t charsBody, const char *body, const char *argNames, const char *compilerOptions, const char *persistOptions);
