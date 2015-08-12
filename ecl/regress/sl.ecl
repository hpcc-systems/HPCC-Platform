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

#option ('globalFold', false);
import lib_stringlib;

stringlib.GetDateYYYYMMDD();
#IF (stringlib.Data2String(x'123456') = '123456')
  true;
#ELSE
  false;
#END
stringlib.Data2String(x'123456') = '123456';
stringlib.StringReverse('abcde') = 'edcba';
stringlib.StringFindReplace('abcde', 'a', 'A') = 'Abcde';
stringlib.StringFindReplace('abcde', 'a', 'AAAAA') = 'AAAAAbcde';
stringlib.StringFindReplace('aaaaa', 'aa', 'a') = 'aaa';
stringlib.StringFindReplace('aaaaa', 'aa', 'b') = 'bba';
stringlib.StringFindReplace('aaaaaa', 'aa', 'b') = 'bbb';
stringlib.StringToLowerCase('ABcde') = 'abcde';
stringlib.StringToUpperCase('ABcde') = 'ABCDE';
stringlib.StringCompareIgnoreCase('ABcdE', 'abcDE') = 0;
stringlib.StringCompareIgnoreCase('ABcdF', 'abcDE') > 0;
stringlib.StringCompareIgnoreCase('ABcdE', 'abcDF') < 0;
stringlib.StringCompareIgnoreCase('ABcdE', 'abcDEF') < 0;
stringlib.StringCompareIgnoreCase('ABcdEF', 'abcDE') > 0;
stringlib.StringCompareIgnoreCase('ABcdE ', 'abcDEF') < 0;
stringlib.StringCompareIgnoreCase('ABcdEF', 'abcDE ') > 0;
stringlib.StringCompareIgnoreCase('ABcdE ', 'abcDE') = 0;
stringlib.StringCompareIgnoreCase('ABcdE', 'abcDE ') = 0;
stringlib.StringFilterOut('abcde', 'cd') = 'abe';
stringlib.StringFilter('abcdeabcdec', 'cd') = 'cdcdc';
stringlib.StringRepad('abc ', 6) + 'd' = 'abc   d';
stringlib.StringFind('abcdef', 'cd', 1) = 3;
stringlib.StringFind('abcdef', 'cd', 2) = 0;
stringlib.StringFind('abccdef', 'cd', 1) = 4;
stringlib.StringFind('abcdcdef', 'cd', 2) = 5;
stringlib.StringUnboundedUnsafeFind('abcdef', 'cd') = 3;
stringlib.StringUnboundedUnsafeFind('abccdef', 'cd') = 4;
stringlib.StringExtract('a,b,c,d', 1) = 'a';
stringlib.StringExtract('a,b,c,d', 2) = 'b';
stringlib.StringExtract('a,b,c,d', 3) = 'c';
stringlib.StringExtract('a,b,c,d', 4) = 'd';
stringlib.StringExtract('a,b,c,d', 5) = '';
stringlib.GetBuildInfo();
