/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestCommonPrefix := MODULE

  EXPORT TestConstant := MODULE

    EXPORT Test01 := ASSERT(Uni.CommonPrefix(u'hello', u'hello', FALSE) = u'hello', CONST);
    EXPORT Test02 := ASSERT(Uni.CommonPrefix(u'hello', u'hello', TRUE) = u'hello', CONST);
    EXPORT Test03 := ASSERT(Uni.CommonPrefix(u'Hello', u'hello', FALSE) = u'', CONST);
    EXPORT Test04 := ASSERT(Uni.CommonPrefix(u'Hello', u'hello', TRUE) = u'Hello', CONST);
    EXPORT Test05 := ASSERT(Uni.CommonPrefix(u'hello', u'', FALSE) = u'', CONST);
    EXPORT Test06 := ASSERT(Uni.CommonPrefix(u'', u'', FALSE) = u'', CONST);
    EXPORT Test07 := ASSERT(Uni.CommonPrefix(u'hello', u'helicopter', FALSE) = u'hel', CONST);
    EXPORT Test08 := ASSERT(Uni.CommonPrefix(u'Hello', u'helicopter', TRUE) = u'Hel', CONST);
    EXPORT Test09 := ASSERT(Uni.CommonPrefix(u'abc', u'xyz', FALSE) = u'', CONST);
    EXPORT Test10 := ASSERT(Uni.CommonPrefix(u'你好世界', u'你好朋友', FALSE) = u'你好', CONST);
    EXPORT Test11 := ASSERT(Uni.CommonPrefix(u'你好世界', u'你好朋友', TRUE) = u'你好', CONST);
    EXPORT Test12 := ASSERT(Uni.CommonPrefix(u'hello', u'hello world', FALSE) = u'hello', CONST);
    EXPORT Test13 := ASSERT(Uni.CommonPrefix(u'abcdefghij', u'abcxyz', FALSE) = u'abc', CONST);
    EXPORT Test14 := ASSERT(Uni.CommonPrefix(u'hello你好', u'hello你朋友', FALSE) = u'hello你', CONST);
    EXPORT Test15 := ASSERT(Uni.CommonPrefix(u'Hello你好', u'hello你朋友', TRUE) = u'Hello你', CONST);

  END;

END;
