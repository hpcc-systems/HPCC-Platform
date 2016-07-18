/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

STRING5 stored_Gavin := 'Gavin' : STORED('stored_Gavin');

EXPORT TestFindWord := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.FindWord('Gavin is cool', 'Gavin'), CONST);
    EXPORT Test02 := ASSERT(NOT Str.FindWord('Gavin is cool', 'gavin'), CONST);
    EXPORT Test03 := ASSERT(Str.FindWord('Gavin is cool', 'gavin', TRUE), CONST);
    EXPORT Test04 := ASSERT(Str.FindWord('Gavin is cool', ' is '), CONST);
    EXPORT Test05 := ASSERT(Str.FindWord('Gavin is cool', 'cool'), CONST);
    EXPORT Test06 := ASSERT(Str.FindWord('Gavin is cool', ''), CONST);
    EXPORT Test07 := ASSERT(Str.FindWord('Gavin is cool', ' '), CONST);
    EXPORT Test08 := ASSERT(NOT Str.FindWord('Gavin, is cool', ','), CONST);
    EXPORT Test09 := ASSERT(NOT Str.FindWord('Gavin , is cool', ','), CONST);
    EXPORT Test10 := ASSERT(Str.FindWord('Gavin, is cool', 'Gavin'), CONST);
  END;

  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Str.FindWord('Gavin is cool', stored_Gavin));
  END;

END;
