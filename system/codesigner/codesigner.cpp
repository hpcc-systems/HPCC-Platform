#include "gpgcodesigner.hpp"

extern jlib_decl ICodeSigner &queryCodeSigner()
{
    return queryGpgCodeSigner();
}
