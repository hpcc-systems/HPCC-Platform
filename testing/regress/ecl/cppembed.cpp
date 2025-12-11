#include <string>

#include "cppembed.hpp"

void mol(size32_t &__lenResult, char * &__result)
{
    std::string mol = "MOL is 42";
    const size32_t len = static_cast<size32_t>(mol.size());
    char * out = (char*)rtlMalloc(len);
    memcpy(out, mol.data(), len);
    __lenResult = len;
    __result = out;
}
