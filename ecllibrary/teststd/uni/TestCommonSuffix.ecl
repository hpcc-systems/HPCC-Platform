/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestCommonSuffix := MODULE

  EXPORT TestConstant := MODULE

    EXPORT Test01 := ASSERT(Uni.CommonSuffix(u'world', u'world', FALSE) = u'world', CONST);
    EXPORT Test02 := ASSERT(Uni.CommonSuffix(u'world', u'world', TRUE) = u'world', CONST);
    EXPORT Test03 := ASSERT(Uni.CommonSuffix(u'World', u'world', FALSE) = u'orld', CONST);
    EXPORT Test04 := ASSERT(Uni.CommonSuffix(u'World', u'world', TRUE) = u'World', CONST);
    EXPORT Test05 := ASSERT(Uni.CommonSuffix(u'World', u'', FALSE) = u'', CONST);
    EXPORT Test06 := ASSERT(Uni.CommonSuffix(u'', u'', FALSE) = u'', CONST);
    EXPORT Test07 := ASSERT(Uni.CommonSuffix(u'helloworld', u'myworld', FALSE) = u'world', CONST);
    EXPORT Test08 := ASSERT(Uni.CommonSuffix(u'helloworld', u'myWORLD', TRUE) = u'world', CONST);
    EXPORT Test09 := ASSERT(Uni.CommonSuffix(u'abc', u'xyz', FALSE) = u'', CONST);
    EXPORT Test10 := ASSERT(Uni.CommonSuffix(u'你好世界', u'朋友世界', FALSE) = u'世界', CONST);
    EXPORT Test11 := ASSERT(Uni.CommonSuffix(u'你好世界', u'朋友世界', TRUE) = u'世界', CONST);
    EXPORT Test12 := ASSERT(Uni.CommonSuffix(u'world', u'helloworld', FALSE) = u'world', CONST);
    EXPORT Test13 := ASSERT(Uni.CommonSuffix(u'abcdefghij', u'xyzghij', FALSE) = u'ghij', CONST);
    EXPORT Test14 := ASSERT(Uni.CommonSuffix(u'hello世界', u'my世界', FALSE) = u'世界', CONST);
    EXPORT Test15 := ASSERT(Uni.CommonSuffix(u'hello世界', u'my世界', TRUE) = u'世界', CONST);

    EXPORT Test16 := ASSERT(Uni.CommonSuffix(u8'world', u8'world', FALSE) = u8'world', CONST);
    EXPORT Test17 := ASSERT(Uni.CommonSuffix(u8'world', u8'world', TRUE) = u8'world', CONST);
    EXPORT Test18 := ASSERT(Uni.CommonSuffix(u8'World', u8'world', FALSE) = u8'orld', CONST);
    EXPORT Test19 := ASSERT(Uni.CommonSuffix(u8'World', u8'world', TRUE) = u8'World', CONST);
    EXPORT Test20 := ASSERT(Uni.CommonSuffix(u8'World', u8'', FALSE) = u8'', CONST);
    EXPORT Test21 := ASSERT(Uni.CommonSuffix(u8'', u8'', FALSE) = u8'', CONST);
    EXPORT Test22 := ASSERT(Uni.CommonSuffix(u8'helloworld', u8'myworld', FALSE) = u8'world', CONST);
    EXPORT Test23 := ASSERT(Uni.CommonSuffix(u8'helloworld', u8'myWORLD', TRUE) = u8'world', CONST);
    EXPORT Test24 := ASSERT(Uni.CommonSuffix(u8'abc', u8'xyz', FALSE) = u8'', CONST);
    EXPORT Test25 := ASSERT(Uni.CommonSuffix(u8'你好世界', u8'朋友世界', FALSE) = u8'世界', CONST);
    EXPORT Test26 := ASSERT(Uni.CommonSuffix(u8'你好世界', u8'朋友世界', TRUE) = u8'世界', CONST);
    EXPORT Test27 := ASSERT(Uni.CommonSuffix(u8'world', u8'helloworld', FALSE) = u8'world', CONST);
    EXPORT Test28 := ASSERT(Uni.CommonSuffix(u8'abcdefghij', u8'xyzghij', FALSE) = u8'ghij', CONST);
    EXPORT Test29 := ASSERT(Uni.CommonSuffix(u8'hello世界', u8'my世界', FALSE) = u8'世界', CONST);
    EXPORT Test30 := ASSERT(Uni.CommonSuffix(u8'hello世界', u8'my世界', TRUE) = u8'世界', CONST);

  END;

END;
