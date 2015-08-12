/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.  All rights reserved.
############################################################################## */
IMPORT Std.Metaphone;

EXPORT TestMetaphone := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Metaphone.primary('Algernon') = 'ALKRNN');
    EXPORT Test02 := ASSERT(Metaphone.secondary('Algernon') = 'ALJRNN');
    EXPORT Test03 := ASSERT(Metaphone.double('Algernon') = 'ALKRNNALJRNN');
  END;

  EXPORT Main := [EVALUATE(TestConst)];
END;