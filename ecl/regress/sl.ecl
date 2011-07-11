/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
