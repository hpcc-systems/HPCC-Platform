/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

import lib_stringlib;

string const_a := 'a' : stored('const_a');

testStringlib := MODULE
  EXPORT AaaStartup := OUTPUT('Begin test');

  EXPORT Test0 := 'This should not be output by evaluate';

  EXPORT TestConstant := MODULE
    EXPORT Test1 := ASSERT(stringLib.StringCompareIgnoreCase('a', 'a') = 0, CONST);
    EXPORT Test2 := ASSERT(stringLib.StringCompareIgnoreCase('a', 'A') = 0, CONST);
    EXPORT Test3 := ASSERT(stringLib.StringCompareIgnoreCase('', '') = 0, CONST);
    EXPORT Test4 := ASSERT(stringLib.StringCompareIgnoreCase('a', 'Z') < 0, CONST);
    EXPORT Test5 := ASSERT(stringLib.StringCompareIgnoreCase('A', 'z') < 0, CONST);
    EXPORT Test6 := ASSERT(stringLib.StringCompareIgnoreCase('a', 'aa') < 0, CONST);
  END;

  EXPORT TestOther := MODULE
    EXPORT Test11 := ASSERT(stringLib.StringCompareIgnoreCase(const_a, 'a') = 0);
    EXPORT Test12 := ASSERT(stringLib.StringCompareIgnoreCase(const_a, 'A') = 0);
    EXPORT Test13 := ASSERT(stringLib.StringCompareIgnoreCase('', '') = 0);
    EXPORT Test14 := ASSERT(stringLib.StringCompareIgnoreCase(const_a, 'Z') < 0);
    EXPORT Test15 := ASSERT(stringLib.StringCompareIgnoreCase(const_a, 'z') > 0);
    EXPORT Test16 := ASSERT(stringLib.StringCompareIgnoreCase(const_a, 'aa') < 0);
  END;

  EXPORT ZzzClosedown := OUTPUT('End test');
END;

testAllPlugins := MODULE
  EXPORT testStringLib := testStringLib;
END;

EVALUATE(testAllPlugins);
