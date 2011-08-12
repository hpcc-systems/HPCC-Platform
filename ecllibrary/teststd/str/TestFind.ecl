/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

STRING stored_blank := '' : STORED('stored_blank');
STRING5 stored_Gavin := 'Gavin' : STORED('stored_Gavin');

EXPORT TestFind := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.Find('Gavin Gavin', 'Gavin', 1) = 1, CONST);
    EXPORT Test02 := ASSERT(Str.Find('Gavin Gavin', 'Gavin', 2) = 7, CONST);
    EXPORT Test03 := ASSERT(Str.Find('Gavin Gavin', 'Gavin', 3) = 0, CONST);
    //Don't read too much
    EXPORT Test04 := ASSERT(Str.Find(('Gavin Gavin')[1..10], 'Gavin', 2) = 0, CONST);
    //Needs to be case significant
    EXPORT Test05 := ASSERT(Str.Find('Gavin Gavin', 'GAVIN', 1) = 0, CONST);
    EXPORT Test06 := ASSERT(Str.Find('Gavin Gavin', '', 1) = 1, CONST);     // What should this return
    EXPORT Test07 := ASSERT(Str.Find('Gavin Gavin', '', 2) = 2, CONST);
    EXPORT Test08 := ASSERT(Str.Find('', 'a', 1) = 0, CONST);
    EXPORT Test09 := ASSERT(Str.Find('', '', 1) = 1, CONST);                    // 
    EXPORT Test09a := ASSERT(Str.Find('', '', 2) = 0, CONST);
    //Check that the next match starts after the previous match
    EXPORT Test10 := ASSERT(Str.Find('xx', 'xxx', 1) = 0, CONST);
    EXPORT Test11 := ASSERT(Str.Find('xxxxx', 'xx', 1) = 1, CONST);
    EXPORT Test12 := ASSERT(Str.Find('xxxxx', 'xx', 2) = 3, CONST);
    EXPORT Test13 := ASSERT(Str.Find('xxxxx', 'xx', 3) = 0, CONST);
    EXPORT Test14 := ASSERT(Str.Find('xx', 'xxx', 1) = 0, CONST);
    //Don't stop for 0 bytes
    EXPORT Test15 := ASSERT(Str.Find('xx' +x'00' + 'xx', 'xx', 1) = 1, CONST);
    EXPORT Test16 := ASSERT(Str.Find('xx' +x'00' + 'xx', 'xx', 2) = 4, CONST);
    
    EXPORT Test17 := ASSERT(Str.Find(' Gavin ', 'Gavin', 1) = 2, CONST);
    EXPORT Test18 := ASSERT(Str.Find(' Gavin', 'Gavin ', 1) = 0, CONST);
    EXPORT Test19 := ASSERT(Str.Find(' Gavin Gavin ', ' ', 1) = 1, CONST);
    EXPORT Test20 := ASSERT(Str.Find(' Gavin Gavin ', ' ', 2) = 7, CONST);
    EXPORT Test21 := ASSERT(Str.Find(' Gavin Gavin ', ' ', 3) = 13, CONST);
    EXPORT Test22 := ASSERT(Str.Find(' Gavin Gavin ', ' ', 4) = 0, CONST);
  END;

  EXPORT TestRuntime := MODULE
    EXPORT Test4 := ASSERT(Str.Find((stored_Gavin + ' ' + stored_Gavin)[1..10], 'Gavin', 2) = 0);
  END;

END;
