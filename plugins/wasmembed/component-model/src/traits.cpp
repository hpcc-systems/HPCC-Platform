#include "traits.hpp"

namespace cmcpp
{
    WasmValVectorIterator::WasmValVectorIterator(const WasmValVector &v) : it(v.begin()), end(v.end()) {}
}
