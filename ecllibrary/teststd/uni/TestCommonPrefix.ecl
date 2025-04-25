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

    EXPORT Test16 := ASSERT(Uni.CommonPrefix(u8'hello', u8'hello', FALSE) = u8'hello', CONST);
    EXPORT Test17 := ASSERT(Uni.CommonPrefix(u8'hello', u8'hello', TRUE) = u8'hello', CONST);
    EXPORT Test18 := ASSERT(Uni.CommonPrefix(u8'Hello', u8'hello', FALSE) = u8'', CONST);
    EXPORT Test19 := ASSERT(Uni.CommonPrefix(u8'Hello', u8'hello', TRUE) = u8'Hello', CONST);
    EXPORT Test20 := ASSERT(Uni.CommonPrefix(u8'hello', u8'', FALSE) = u8'', CONST);
    EXPORT Test21 := ASSERT(Uni.CommonPrefix(u8'', u8'', FALSE) = u8'', CONST);
    EXPORT Test22 := ASSERT(Uni.CommonPrefix(u8'hello', u8'helicopter', FALSE) = u8'hel', CONST);
    EXPORT Test23 := ASSERT(Uni.CommonPrefix(u8'Hello', u8'helicopter', TRUE) = u8'Hel', CONST);
    EXPORT Test24 := ASSERT(Uni.CommonPrefix(u8'abc', u8'xyz', FALSE) = u8'', CONST);
    EXPORT Test25 := ASSERT(Uni.CommonPrefix(u8'你好世界', u8'你好朋友', FALSE) = u8'你好', CONST);
    EXPORT Test26 := ASSERT(Uni.CommonPrefix(u8'你好世界', u8'你好朋友', TRUE) = u8'你好', CONST);
    EXPORT Test27 := ASSERT(Uni.CommonPrefix(u8'hello', u8'hello world', FALSE) = u8'hello', CONST);
    EXPORT Test28 := ASSERT(Uni.CommonPrefix(u8'abcdefghij', u8'abcxyz', FALSE) = u8'abc', CONST);
    EXPORT Test29 := ASSERT(Uni.CommonPrefix(u8'hello你好', u8'hello你朋友', FALSE) = u8'hello你', CONST);
    EXPORT Test30 := ASSERT(Uni.CommonPrefix(u8'Hello你好', u8'hello你朋友', TRUE) = u8'Hello你', CONST);

  END;

END;
