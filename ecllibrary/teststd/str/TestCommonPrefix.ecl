/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestCommonPrefix := MODULE

  EXPORT TestConstant := MODULE

    EXPORT Test01 := ASSERT(Str.CommonPrefix('DANIEL', 'DANNY') = 'DAN', CONST);
    EXPORT Test02 := ASSERT(Str.CommonPrefix('DANIEL', 'FUBAR') = '', CONST);
    EXPORT Test03 := ASSERT(Str.CommonPrefix('DAN', 'DANNY') = 'DAN', CONST);
    EXPORT Test04 := ASSERT(Str.CommonPrefix('DANIEL', 'DAN') = 'DAN', CONST);
    EXPORT Test05 := ASSERT(Str.CommonPrefix('DANIEL', '') = '', CONST);
    EXPORT Test06 := ASSERT(Str.CommonPrefix('', 'DANNY') = '', CONST);

    EXPORT Test07 := ASSERT(Str.CommonPrefix('DANIEL', 'Danny', TRUE) = 'DAN', CONST);
    EXPORT Test08 := ASSERT(Str.CommonPrefix('DANIEL', 'daniel', TRUE) = 'DANIEL', CONST);
    EXPORT Test09 := ASSERT(Str.CommonPrefix('Dan', 'DANNY', TRUE) = 'Dan', CONST);
    EXPORT Test10 := ASSERT(Str.CommonPrefix('daniel', 'DAN', TRUE) = 'dan', CONST);

  END;

END;
