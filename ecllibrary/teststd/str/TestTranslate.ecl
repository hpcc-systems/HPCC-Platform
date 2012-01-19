/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestTranslate := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Str.Translate(' ABCDEF FEDCBA ', 'A', 'Z')+'X' = ' ZBCDEF FEDCBZ X', CONST);
    EXPORT Test02 := ASSERT(Str.Translate(' ABCDEF FEDCBA ', 'A', '')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test03 := ASSERT(Str.Translate(' ABCDEF FEDCBA ', 'A', 'ZZ')+'X' = ' ABCDEF FEDCBA X', CONST);
    EXPORT Test04 := ASSERT(Str.Translate(' ABCDEF FEDCBA ', 'AB', '$!')+'X' = ' $!CDEF FEDC!$ X', CONST);
    EXPORT Test05 := ASSERT(Str.Translate(' \377ABCDEF FEDCBA ', 'AB', '$!')+'X' = ' \377$!CDEF FEDC!$ X', CONST);
    EXPORT Test06 := ASSERT(Str.Translate(' ABCDEF FEDCBA ', 'AAA', '!%$')+'X' = ' $BCDEF FEDCB$ X', CONST);
  END;

END;
