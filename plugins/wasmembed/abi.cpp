/*
  See:  https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md
        https://github.com/WebAssembly/component-model/blob/main/design/mvp/canonical-abi/definitions.py
*/

#include "abi.hpp"

#include "jexcept.hpp"

auto UTF16_TAG = 1U << 31;

//
/* canonical despecialize (python)          -------------------------------------------------------------

def despecialize(t):
  match t:
    case Tuple(ts)         : return Record([ Field(str(i), t) for i,t in enumerate(ts) ])
    case Union(ts)         : return Variant([ Case(str(i), t) for i,t in enumerate(ts) ])
    case Enum(labels)      : return Variant([ Case(l, None) for l in labels ])
    case Option(t)         : return Variant([ Case("none", None), Case("some", t) ])
    case Result(ok, error) : return Variant([ Case("ok", ok), Case("error", error) ])
    case _                 : return t

*/

// template <typename T>
// wasmtime::ValType despecialize(const T<wasmtime::Val> &t)
// {
//   switch (t.kind())
//   {
//   case wasmtime::ValKind::I32:
//   case wasmtime::ValKind::I64:
//   case wasmtime::ValKind::F32:
//   case wasmtime::ValKind::F64:
//   case wasmtime::ValKind::V128:
//     return t.kind();
//   default:
//     return wasmtime::ValType::i32();
//   }
// }

/* canonical alignment (python)             -------------------------------------------------------------

def alignment(t):
  match despecialize(t):
    case Bool()             : return 1
    case S8() | U8()        : return 1
    case S16() | U16()      : return 2
    case S32() | U32()      : return 4
    case S64() | U64()      : return 8
    case Float32()          : return 4
    case Float64()          : return 8
    case Char()             : return 4
    case String() | List(_) : return 4
    case Record(fields)     : return alignment_record(fields)
    case Variant(cases)     : return alignment_variant(cases)
    case Flags(labels)      : return alignment_flags(labels)
    case Own(_) | Borrow(_) : return 4

*/

// int alignment(const wasmtime::ValType &t)
// {
//   switch (t.kind())
//   {
//   case wasmtime::ValKind::I32:
//   case wasmtime::ValKind::F32:
//     return 4;
//   case wasmtime::ValKind::I64:
//   case wasmtime::ValKind::F64:
//     return 8;
//   case wasmtime::ValKind::V128:
//     return 16;
//   default:
//     return 1;
//   }
// }

/* canonical align_to (python)              -------------------------------------------------------------

def align_to(ptr, alignment):
  return math.ceil(ptr / alignment) * alignment

*/

uint32_t align_to(uint32_t ptr, uint32_t alignment)
{
    return (ptr + alignment - 1) & ~(alignment - 1);
}

bool isAligned(uint32_t ptr, uint32_t alignment)
{
    return (ptr & (alignment - 1)) == 0;
}

//  loading ---

/* canonical load_int (python)              -------------------------------------------------------------

def load_int(cx, ptr, nbytes, signed = False):
  return int.from_bytes(cx.opts.memory[ptr : ptr+nbytes], 'little', signed=signed)

*/

template <typename T>
T load_int(const wasmtime::Span<uint8_t> &data, uint32_t ptr)
{
    T retVal = 0;
    if constexpr (sizeof(T) == 1)
    {
        retVal = static_cast<T>(data[ptr]);
    }
    else if constexpr (sizeof(T) == 2)
    {
        retVal = static_cast<T>((static_cast<uint16_t>(data[ptr + 1]) << 8) |
                                static_cast<uint16_t>(data[ptr]));
    }
    else if constexpr (sizeof(T) == 4)
    {
        retVal = static_cast<T>((static_cast<uint32_t>(data[ptr + 3]) << 24) |
                                (static_cast<uint32_t>(data[ptr + 2]) << 16) |
                                (static_cast<uint32_t>(data[ptr + 1]) << 8) |
                                static_cast<uint32_t>(data[ptr]));
    }
    else if constexpr (sizeof(T) == 8)
    {
        retVal = static_cast<T>((static_cast<uint64_t>(data[ptr + 7]) << 56) |
                                (static_cast<uint64_t>(data[ptr + 6]) << 48) |
                                (static_cast<uint64_t>(data[ptr + 5]) << 40) |
                                (static_cast<uint64_t>(data[ptr + 4]) << 32) |
                                (static_cast<uint64_t>(data[ptr + 3]) << 24) |
                                (static_cast<uint64_t>(data[ptr + 2]) << 16) |
                                (static_cast<uint64_t>(data[ptr + 1]) << 8) |
                                static_cast<uint64_t>(data[ptr]));
    }
    return retVal;
}
/* canonical load_string_from_range (python)  -------------------------------------------------------------

def load_string_from_range(cx, ptr, tagged_code_units):
  match cx.opts.string_encoding:
    case 'utf8':
      alignment = 1
      byte_length = tagged_code_units
      encoding = 'utf-8'
    case 'utf16':
      alignment = 2
      byte_length = 2 * tagged_code_units
      encoding = 'utf-16-le'
    case 'latin1+utf16':
      alignment = 2
      if bool(tagged_code_units & UTF16_TAG):
        byte_length = 2 * (tagged_code_units ^ UTF16_TAG)
        encoding = 'utf-16-le'
      else:
        byte_length = tagged_code_units
        encoding = 'latin-1'

  trap_if(ptr != align_to(ptr, alignment))
  trap_if(ptr + byte_length > len(cx.opts.memory))
  try:
    s = cx.opts.memory[ptr : ptr+byte_length].decode(encoding)
  except UnicodeError:
    trap()

  return (s, cx.opts.string_encoding, tagged_code_units)

*/

//  More:  Not currently available from the wasmtime::context object, see https://github.com/bytecodealliance/wasmtime/issues/6719
static const std::string global_encoding = "utf8";

std::tuple<uint32_t /*ptr*/, std::string /*encoding*/, uint32_t /*byte length*/> load_string_from_range(const wasmtime::Span<uint8_t> &data, uint32_t ptr, uint32_t tagged_code_units)
{
    std::string encoding = "utf-8";
    uint32_t byte_length = tagged_code_units;
    uint32_t alignment = 1;
    if (global_encoding.compare("utf8") == 0)
    {
        alignment = 1;
        byte_length = tagged_code_units;
        encoding = "utf-8";
    }
    else if (global_encoding.compare("utf16") == 0)
    {
        alignment = 2;
        byte_length = 2 * tagged_code_units;
        encoding = "utf-16-le";
    }
    else if (global_encoding.compare("latin1+utf16") == 0)
    {
        alignment = 2;
        if (tagged_code_units & UTF16_TAG)
        {
            byte_length = 2 * (tagged_code_units ^ UTF16_TAG);
            encoding = "utf-16-le";
        }
        else
        {
            byte_length = tagged_code_units;
            encoding = "latin-1";
        }
    }

    if (!isAligned(ptr, alignment))
    {
        throw makeStringException(3, "Invalid alignment");
    }

    if (ptr + byte_length > data.size())
    {
        throw makeStringException(1, "Out of bounds");
    }

    return std::make_tuple(ptr, encoding, byte_length);
}

/*  canonical load_string (python)          -------------------------------------------------------------

def load_string(cx, ptr):
  begin = load_int(cx, ptr, 4)
  tagged_code_units = load_int(cx, ptr + 4, 4)
  return load_string_from_range(cx, begin, tagged_code_units)

*/
std::tuple<uint32_t /*ptr*/, std::string /*encoding*/, uint32_t /*byte length*/> load_string(const wasmtime::Span<uint8_t> &data, uint32_t ptr)
{
    uint32_t begin = load_int<uint32_t>(data, ptr);
    uint32_t tagged_code_units = load_int<uint32_t>(data, ptr + 4);
    return load_string_from_range(data, begin, tagged_code_units);
}

/*  canonical load_list_from_range (python) -------------------------------------------------------------

def load_list_from_range(cx, ptr, length, elem_type):
  trap_if(ptr != align_to(ptr, alignment(elem_type)))
  trap_if(ptr + length * size(elem_type) > len(cx.opts.memory))
  a = []
  for i in range(length):
    a.append(load(cx, ptr + i * size(elem_type), elem_type))
  return a

*/

template <typename T>
std::vector<T> load_list_from_range(const wasmtime::Span<uint8_t> &data, uint32_t ptr, uint32_t length)
{
    if (!isAligned(ptr, alignment(T{})))
        throw makeStringException(2, "Pointer is not aligned");
    if (ptr + length * sizeof(T) > data.size())
        throw makeStringException(1, "Out of bounds access");
    std::vector<T> a;
    for (uint32_t i = 0; i < length; i++)
    {
        a.push_back(load<T>(data, ptr + i * sizeof(T)));
    }
    return a;
}

/*  canonical load_list (python)            -------------------------------------------------------------

def load_list(cx, ptr, elem_type):
  begin = load_int(cx, ptr, 4)
  length = load_int(cx, ptr + 4, 4)
  return load_list_from_range(cx, begin, length, elem_type)

*/

//  Storing  ---
