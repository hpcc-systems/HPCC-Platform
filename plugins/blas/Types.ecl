// Types for the Block Basic Linear Algebra Sub-programs support
// WARNING: attributes can not be changed without making
//corresponding changes to the C++ attributes.
EXPORT Types := MODULE
  EXPORT dimension_t  := UNSIGNED4;     // WARNING: type used in C++ attributes
  EXPORT value_t      := REAL8;         // Warning: type used in C++ attribute
  EXPORT matrix_t     := SET OF REAL8;  // Warning: type used in C++ attribute
  EXPORT Triangle     := ENUM(UNSIGNED1, Upper=1, Lower=2); //Warning
  EXPORT Diagonal     := ENUM(UNSIGNED1, UnitTri=1, NotUnitTri=2);  //Warning
  EXPORT Side         := ENUM(UNSIGNED1, Ax=1, xA=2);  //Warning
END;