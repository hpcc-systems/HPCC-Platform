# Type System

> Last verified: 2026-05

## Overview
The type system in eclcc is implemented in `common/deftype/` and provides the foundation for type checking, type promotion, serialization format decisions, and code generation.

## Source Files
| File | Purpose |
|------|---------|
| `common/deftype/deftype.hpp` | `ITypeInfo` interface and type factory functions |
| `common/deftype/deftype.cpp` | Type class implementations |
| `common/deftype/deftype.ipp` | Internal implementation details |
| `common/deftype/defvalue.hpp` | `IValue` interface for compile-time constant values |
| `common/deftype/defvalue.cpp` | Value class implementations |
| `common/deftype/defvalue.ipp` | Internal value details |
| `common/deftype/deffield.hpp` | Field definition interfaces |
| `common/deftype/deffield.cpp` | Field class implementations |
| `common/deftype/deffield.ipp` | Internal field details |

## ITypeInfo Interface
The primary type interface. Key methods:
- `getTypeCode()` — returns `type_t` enum value identifying the type kind
- `getSize()` — physical size in bytes (UNKNOWN_LENGTH if variable)
- `getStringLen()` — logical length (for strings: character count)
- `queryCharset()` — character set for string types
- `queryCollation()` — collation for string types
- `queryLocale()` — locale information
- `queryChildType()` — element type for sets/arrays
- `queryPromotedType()` — the canonical promoted form of this type
- `assignableFrom(other)` — can a value of `other` type be assigned to this type?
- `castFrom(...)` — create a value by casting from raw data

## Type Kinds (`type_t` enum)
Major categories:
- **Scalar**: `type_boolean`, `type_int`, `type_real`, `type_decimal`, `type_string`, `type_unicode`, `type_data`, `type_varstring`, `type_varunicode`, `type_utf8`
- **Composite**: `type_record`, `type_row`, `type_table`, `type_groupedtable`, `type_set`, `type_dictionary`
- **Special**: `type_void`, `type_null`, `type_any`, `type_alien`, `type_enumerated`, `type_pattern`, `type_rule`, `type_token`, `type_feature`, `type_event`, `type_function`, `type_scope`

## Type Factories
Types are created via factory functions that ensure commoning (like expressions):
- `makeIntType(size, isSigned)`
- `makeRealType(size)`
- `makeStringType(len, charset, collation)`
- `makeUnicodeType(len, locale)`
- `makeBoolType()`
- `makeDataType(size)`
- `makeRecordType()` — starts building a record type
- `makeTableType(recordType)` — dataset of records
- `makeSetType(elementType)`
- etc.

## Type Promotion
When combining values of different types (e.g., in comparisons or arithmetic), types are promoted to a common type. `queryPromotedType()` returns the canonical promoted form.

## Records and Fields
- Records are types containing ordered field definitions.
- Fields have a name, type, and optional default value.
- `IHqlSimpleScope` resolves field names within records.
- IFBLOCKs: conditional fields are flattened into the containing record's scope.

## Type Information in Expression Graph
Dataset expressions carry type information that includes:
- **Record structure**: defines the fields
- **Grouping**: whether the dataset is grouped
- **Sort order**: current ordering
- **Distribution**: partitioning information

These are accessed via helper functions (`isGrouped(expr)`, etc.) rather than direct type interrogation. Future plan: move grouping/sort/distribution from type to general derived properties.

## Serialization
The type system drives serialization decisions:
- Fixed-size vs variable-size encoding
- String encoding (ASCII, Unicode, UTF-8)
- Alignment and packing
- Record format CRCs for compatibility checking
