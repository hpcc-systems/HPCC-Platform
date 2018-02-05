/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Str;

EXPORT TestTrim := MODULE

  EXPORT TestConstant := [
    ASSERT((>DATA<)TRIM('   ') != (>DATA<)'    ', CONST);
    ASSERT((>DATA<)TRIM('   ') = (>DATA<)'', CONST);
    ASSERT((>DATA<)TRIM(' abc   ') = (>DATA<)' abc', CONST);
    ASSERT((>DATA<)TRIM(' abc  def ') = (>DATA<)' abc  def', CONST);
    ASSERT((>DATA<)TRIM(' abc  def ', LEFT) = (>DATA<)'abc  def ', CONST);
    ASSERT((>DATA<)TRIM(' abc  def ', RIGHT) = (>DATA<)' abc  def', CONST);
    ASSERT((>DATA<)TRIM(' abc  def ', LEFT, RIGHT) = (>DATA<)'abc  def', CONST);
    ASSERT((>DATA<)TRIM(' abc  def ', ALL) = (>DATA<)'abcdef', CONST);
    ASSERT((>DATA<)TRIM('\tabc\t\tdef\t') = (>DATA<)'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM('\tabc\t\tdef\t', LEFT) = (>DATA<)'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM('\tabc\t\tdef\t', RIGHT) = (>DATA<)'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM('\tabc\t\tdef\t', LEFT, RIGHT) = (>DATA<)'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM('\tabc\t\tdef\t', ALL) = (>DATA<)'\tabc\t\tdef\t', CONST);
    ASSERT((>DATA<)TRIM('', ALL) = (>DATA<)'', CONST);
    ASSERT((>DATA<)TRIM('\t', ALL) = (>DATA<)'\t', CONST);
    //---------------------------------------
    ASSERT(TRUE)
  ];

END;
