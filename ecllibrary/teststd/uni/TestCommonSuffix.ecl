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

  END;

END;
