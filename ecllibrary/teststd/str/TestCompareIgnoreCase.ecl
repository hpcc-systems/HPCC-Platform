/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str as Str;

STRING stored_blank := '' : STORED('stored_blank');
STRING stored_a := 'a' : STORED('stored_a');
STRING stored_upper_zz := 'ZZ' : STORED('stored_upper_zz');
STRING stored_lower_zz := 'ZZ' : STORED('stored_lower_zz');

EXPORT TestCompareIgnoreCase := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test1 := ASSERT(Str.CompareIgnoreCase('a', 'a') = 0, CONST);
    EXPORT Test2 := ASSERT(Str.CompareIgnoreCase('a', 'A') = 0, CONST);
    EXPORT Test3 := ASSERT(Str.CompareIgnoreCase('', '') = 0, CONST);
    EXPORT Test4 := ASSERT(Str.CompareIgnoreCase('a', 'Z') < 0, CONST);
    EXPORT Test5 := ASSERT(Str.CompareIgnoreCase('A', 'z') < 0, CONST);
    EXPORT Test6 := ASSERT(Str.CompareIgnoreCase('a', 'aa') < 0, CONST);
    EXPORT Test7 := ASSERT(Str.CompareIgnoreCase('Zz', 'zz') = 0, CONST);
    EXPORT Test8 := ASSERT(Str.CompareIgnoreCase('Zz', 'zz') = 0, CONST);
    EXPORT Test9 := ASSERT(Str.CompareIgnoreCase('\\', '{') < 0, CONST);
    EXPORT Test10 := ASSERT(Str.CompareIgnoreCase('ABCDEF   ', 'abcdef') = 0, CONST);
    EXPORT Test11 := ASSERT(Str.CompareIgnoreCase('ABCDEF', 'abcdef   ') = 0, CONST);
    EXPORT Test12 := ASSERT(Str.CompareIgnoreCase('ABCDEF  Z', 'abcdef') > 0, CONST);
    EXPORT Test13 := ASSERT(Str.CompareIgnoreCase('ABCDEF', 'abcdef  Z') < 0, CONST);
  END;
  
  EXPORT TestRuntime := MODULE
    EXPORT Test1 := ASSERT(Str.CompareIgnoreCase(stored_blank + stored_a, 'a') = 0);
    EXPORT Test2 := ASSERT(Str.CompareIgnoreCase(stored_blank + stored_a, 'A') = 0);
    EXPORT Test3 := ASSERT(Str.CompareIgnoreCase(stored_blank + '', '') = 0);
    EXPORT Test4 := ASSERT(Str.CompareIgnoreCase(stored_blank + stored_a, 'Z') < 0);
    EXPORT Test5 := ASSERT(Str.CompareIgnoreCase(stored_blank + stored_a, 'z') < 0);
    EXPORT Test6 := ASSERT(Str.CompareIgnoreCase(stored_blank + 'Z', stored_a) > 0);
    EXPORT Test7 := ASSERT(Str.CompareIgnoreCase(stored_blank + stored_a, 'aa') < 0);
    EXPORT Test8 := ASSERT(Str.CompareIgnoreCase(stored_blank + 'Zz', 'zz') = 0);
    EXPORT Test9 := ASSERT(Str.CompareIgnoreCase(stored_blank + 'Zz', 'zz') = 0);
    EXPORT Test10 := ASSERT(Str.CompareIgnoreCase(stored_blank + '\\', '{') < 0);
  END;
  
END;
