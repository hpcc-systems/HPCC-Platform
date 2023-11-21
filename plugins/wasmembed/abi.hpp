#include <wasmtime.hh>

std::tuple<uint32_t /*ptr*/, std::string /*encoding*/, uint32_t /*byte length*/> load_string(const wasmtime::Span<uint8_t> &data, uint32_t ptr);
