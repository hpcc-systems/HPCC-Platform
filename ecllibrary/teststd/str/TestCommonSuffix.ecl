/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestCommonSuffix := MODULE

  EXPORT TestConstant := MODULE

    EXPORT Test01 := ASSERT(Str.CommonSuffix('LAPTOP', 'TABLETOP') = 'TOP', CONST);
    EXPORT Test02 := ASSERT(Str.CommonSuffix('LAPTOP', 'FUBAR') = '', CONST);
    EXPORT Test03 := ASSERT(Str.CommonSuffix('TABLETOP', 'LAPTOP') = 'TOP', CONST);
    EXPORT Test04 := ASSERT(Str.CommonSuffix('TABLETOP', 'TOP') = 'TOP', CONST);
    EXPORT Test05 := ASSERT(Str.CommonSuffix('TOP', 'LAPTOP') = 'TOP', CONST);
    EXPORT Test06 := ASSERT(Str.CommonSuffix('LAPTOP', '') = '', CONST);
    EXPORT Test07 := ASSERT(Str.CommonSuffix('', 'TABLETOP') = '', CONST);

    EXPORT Test08 := ASSERT(Str.CommonSuffix('LAPTOP', 'Tabletop', TRUE) = 'TOP', CONST);
    EXPORT Test09 := ASSERT(Str.CommonSuffix('LAPTOP', 'laptop', TRUE) = 'LAPTOP', CONST);
    EXPORT Test10 := ASSERT(Str.CommonSuffix('Top', 'LAPTOP', TRUE) = 'Top', CONST);
    EXPORT Test11 := ASSERT(Str.CommonSuffix('laptop', 'TOP', TRUE) = 'top', CONST);

  END;

END;
