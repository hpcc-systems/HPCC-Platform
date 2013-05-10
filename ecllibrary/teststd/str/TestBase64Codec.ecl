/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.  All rights reserved.
############################################################################## */
IMPORT Std.Str;

EXPORT TestBase64Codec := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'ca')) = x'ca');
    EXPORT Test02 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafe')) = x'cafe');
    EXPORT Test03 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafeba')) = x'cafeba');
    EXPORT Test04 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabe')) = x'cafebabe');
    EXPORT Test05 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabeca')) = x'cafebabeca');
    EXPORT Test06 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabecafe')) = x'cafebabecafe');
  END;

  EXPORT Main := [EVALUATE(TestConst)]; 
END;

