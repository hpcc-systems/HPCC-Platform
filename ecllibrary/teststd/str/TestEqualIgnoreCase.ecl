/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str as Str;

STRING stored_blank := '' : STORED('stored_blank');
STRING stored_a := 'a' : STORED('stored_a');
STRING stored_upper_zz := 'ZZ' : STORED('stored_upper_zz');
STRING stored_lower_zz := 'ZZ' : STORED('stored_lower_zz');

EXPORT TestEqualIgnoreCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test1 := ASSERT(Str.EqualIgnoreCase('a', 'a'), CONST);
    EXPORT Test2 := ASSERT(Str.EqualIgnoreCase('a', 'A'), CONST);
    EXPORT Test3 := ASSERT(Str.EqualIgnoreCase('', ''), CONST);
    EXPORT Test4 := ASSERT(NOT Str.EqualIgnoreCase('a', 'Z'), CONST);
    EXPORT Test5 := ASSERT(NOT Str.EqualIgnoreCase('A', 'z'), CONST);
    EXPORT Test6 := ASSERT(NOT Str.EqualIgnoreCase('a', 'aa'), CONST);
    EXPORT Test7 := ASSERT(Str.EqualIgnoreCase('Zz', 'zz'), CONST);
    EXPORT Test8 := ASSERT(Str.EqualIgnoreCase('Zz', 'zz'), CONST);
    EXPORT Test9 := ASSERT(NOT Str.EqualIgnoreCase('\\', '{'), CONST);
  END;
  
  EXPORT TestRuntime := MODULE
    EXPORT Test1 := ASSERT(Str.EqualIgnoreCase(stored_blank + stored_a, 'a'));
    EXPORT Test2 := ASSERT(Str.EqualIgnoreCase(stored_blank + stored_a, 'A'));
    EXPORT Test3 := ASSERT(Str.EqualIgnoreCase(stored_blank + '', ''));
    EXPORT Test4 := ASSERT(NOT Str.EqualIgnoreCase(stored_blank + stored_a, 'Z'));
    EXPORT Test5 := ASSERT(NOT Str.EqualIgnoreCase(stored_blank + stored_a, 'z'));
    EXPORT Test6 := ASSERT(NOT Str.EqualIgnoreCase(stored_blank + stored_a, 'aa'));
    EXPORT Test7 := ASSERT(Str.EqualIgnoreCase(stored_blank + 'Zz', 'zz'));
    EXPORT Test8 := ASSERT(Str.EqualIgnoreCase(stored_blank + 'Zz', 'zz'));
    EXPORT Test9 := ASSERT(NOT Str.EqualIgnoreCase(stored_blank + '\\', '{'));
  END;
  
END;
