/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

UNICODE5 ustored_Gavin := u'Gavin' : STORED('ustored_Gavin');

EXPORT TestFindWord := MODULE

  EXPORT TestConstant := MODULE
    EXPORT Test01 := ASSERT(Uni.FindWord(u'Gavin is cool', u'Gavin'), CONST);
    EXPORT Test02 := ASSERT(NOT Uni.FindWord(u'Gavin is cool', u'gavin'), CONST);
    EXPORT Test03 := ASSERT(Uni.FindWord(u'Gavin is cool', u'gavin', TRUE), CONST);
    EXPORT Test04 := ASSERT(Uni.FindWord(u'Gavin is cool', u' is '), CONST);
    EXPORT Test05 := ASSERT(Uni.FindWord(u'Gavin is cool', u'cool'), CONST);
    EXPORT Test06 := ASSERT(Uni.FindWord(u'Gavin is cool', u''), CONST);
    EXPORT Test07 := ASSERT(Uni.FindWord(u'Gavin is cool', u' '), CONST);
    EXPORT Test08 := ASSERT(NOT Uni.FindWord(u'Gavin, is cool', u','), CONST);
    EXPORT Test09 := ASSERT(NOT Uni.FindWord(u'Gavin , is cool', u','), CONST);
    EXPORT Test10 := ASSERT(Uni.FindWord(u'Gavin, is cool', u'Gavin'), CONST);
  END;

  EXPORT TestRuntime := MODULE
    EXPORT Test01 := ASSERT(Uni.FindWord(u'Gavin is cool', ustored_Gavin));
  END;

END;
