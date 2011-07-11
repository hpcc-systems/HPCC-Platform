/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

STRING stored_blank := '' : STORED('stored_blank');
STRING5 stored_Gavin := 'Gavin' : STORED('stored_Gavin');

EXPORT TestFindCount := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.FindCount('Gavin Gavin', 'Gavin') = 2, CONST);
    //Don't read too much
    EXPORT Test04 := ASSERT(Str.FindCount(('Gavin Gavin')[1..10], 'Gavin') = 1, CONST);
    //Needs to be case significant
    EXPORT Test05 := ASSERT(Str.FindCount('Gavin Gavin', 'GAVIN') = 0, CONST);
    EXPORT Test06 := ASSERT(Str.FindCount('Gavin Gavin', '') = 12);     // What should this return
    EXPORT Test08 := ASSERT(Str.FindCount('', 'a') = 0, CONST);
    EXPORT Test09 := ASSERT(Str.FindCount('', '') = 1, CONST);                  // Not sure about this either!
    //Check that the next match starts after the previous match
    EXPORT Test10 := ASSERT(Str.FindCount('xx', 'xxx') = 0, CONST);
    EXPORT Test11 := ASSERT(Str.FindCount('xxxxx', 'x') = 5, CONST);
    EXPORT Test12 := ASSERT(Str.FindCount('xxxxx', 'xx') = 2, CONST);
    //Don't stop for 0 bytes
    EXPORT Test15 := ASSERT(Str.FindCount('xx' +x'00' + 'xx', 'xx') = 2, CONST);
    
    EXPORT Test17 := ASSERT(Str.FindCount(' Gavin ', 'Gavin') = 1, CONST);
    EXPORT Test18 := ASSERT(Str.FindCount(' Gavin', 'Gavin ') = 0, CONST);
    EXPORT Test19 := ASSERT(Str.FindCount('Gavin Gavin', 'a') = 2, CONST);
  END;

  EXPORT TestRuntime := MODULE
    EXPORT Test4 := ASSERT(Str.FindCount((stored_Gavin + ' ' + stored_Gavin)[1..10], 'Gavin') = 1);
  END;

END;
