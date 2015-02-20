/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.  All rights reserved.
############################################################################## */
IMPORT Std.Metaphone3;

EXPORT TestMetaphone3 := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Metaphone3.primary('Algernon') = 'ALKRNN');
    EXPORT Test02 := ASSERT(Metaphone3.secondary('Algernon') = 'ALJRNN');
    EXPORT Test03 := ASSERT(Metaphone3.double('Algernon') = 'ALKRNNALJRNN');
  END;

  EXPORT Main := [EVALUATE(TestConst)];
END;