/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestTrim := MODULE

  EXPORT TestConstant := [
    ASSERT((>DATA<)TRIM(U'   ') != (>DATA<)U'    ', CONST);
    ASSERT((>DATA<)TRIM(U'   ') = (>DATA<)U'', CONST);
    ASSERT((>DATA<)TRIM(U' abc   ') = (>DATA<)U' abc', CONST);
    ASSERT((>DATA<)TRIM(U' abc  def ') = (>DATA<)U' abc  def', CONST);
    ASSERT((>DATA<)TRIM(U' abc  def ', LEFT) = (>DATA<)U'abc  def ', CONST);
    ASSERT((>DATA<)TRIM(U' abc  def ', RIGHT) = (>DATA<)U' abc  def', CONST);
    ASSERT((>DATA<)TRIM(U' abc  def ', LEFT, RIGHT) = (>DATA<)U'abc  def', CONST);
    ASSERT((>DATA<)TRIM(U' abc  def ', ALL) = (>DATA<)U'abcdef', CONST);
    ASSERT((>DATA<)TRIM(U'\tabc\t\tdef\t') = (>DATA<)U'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM(U'\tabc\t\tdef\t', LEFT) = (>DATA<)U'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM(U'\tabc\t\tdef\t', RIGHT) = (>DATA<)U'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM(U'\tabc\t\tdef\t', LEFT, RIGHT) = (>DATA<)U'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM(U'\tabc\t\tdef\t', ALL) = (>DATA<)U'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM(U'', ALL) = (>DATA<)U'', CONST);
    ASSERT((>DATA<)TRIM(U'\t', ALL) = (>DATA<)U'\t', CONST);
    //---------------------------------------
    ASSERT(TRUE)
  ];

END;
