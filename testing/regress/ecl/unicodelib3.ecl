/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

IMPORT * FROM lib_unicodelib;

//#IF usage 

UNICODE10       F1   := U'ABCDEABCDE';
UNICODE5        F2   := U'ABCDE';
UNICODE2        F3   := U'BD';
UNICODE2        F4   := U'BC';
UNICODE1        F5   := U'X';
UNICODE7        F6   := U'AB,CD,E';
UNICODE5        F7   := U'abcde';
DATA2           CRLF := x'0a0d';

#IF(UnicodeLib.UnicodeFilterOut(F2, F3) = U'ACE')
output('UnicodeLib.UnicodeFilterOut(const unicode src, const unicode _within)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeFilterOut(const unicode src, const unicode _within)'); 
#END

#IF(UnicodeLib.UnicodeFilter(F2, F3) = U'BD')
output('UnicodeLib.UnicodeFilter(const unicode src, const unicode _within)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeFilter(const unicode src, const unicode _within)'); 
#END

#IF(UnicodeLib.UnicodeSubstituteOut(F2, F3, F5) = U'AXCXE')
output('UnicodeLib.UnicodeSubstituteOut(const unicode src, const unicode _within, const unicode _newchar)'); 
#ELSE
output('UnicodeLib.FAILED UnicodeSubstituteOut(const unicode src, const unicode _within, const unicode _newchar)'); 
#END

#IF(UnicodeLib.UnicodeSubstitute(F2, F3, F5) = U'XBXDX')
output('UnicodeLib.UnicodeSubstitute(const unicode src, const unicode _within, const unicode _newchar)'); 
#ELSE
output('UnicodeLib.FAILED UnicodeSubstitute(const unicode src, const unicode _within, const unicode _newchar)'); 
#END

#IF(UnicodeLib.UnicodeRepad(U'ABCDE   ', 6) = U'ABCDE ')
output('UnicodeLib.UnicodeRepad(const unicode src, unsigned4 size)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeRepad(const unicode src, unsigned4 size)'); 
#END

#IF(UnicodeLib.UnicodeFind(F2, F4, 1) = 2)
output('UnicodeLib.UnicodeFind(const unicode src, const unicode tofind, unsigned4 instance)');
#ELSE
output('FAILED UnicodeLib.UnicodeFind(const unicode src, const unicode tofind, unsigned4 instance)');
#END
/*
#IF(UnicodeLib.UnicodeLocaleFind(F2, F4, 1, V'en_us' = 2)
output('UnicodeLib.UnicodeLocaleFind(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleFind(const FAILED unicode src, const unicode tofind, unsigned4 instance, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleFindAtStrength(F2, F4, 1, V'en_us', 1) = 2)
output('UnicodeLib.UnicodeLocaleFindAtStrength(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename, integer1 strength)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleFindAtStrength(const unicode src, const FAILED unicode tofind, unsigned4 instance, const varstring localename, integer1 strength)'); 
#END
*/
#IF(UnicodeLib.UnicodeExtract(F6, 2) = U'CD')
output('UnicodeLib.UnicodeExtract(const unicode src, unsigned4 instance)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeExtract(const unicode src, unsigned4 instance)'); 
#END

#IF(UnicodeLib.UnicodeExtract50(F6, 2) = U'CD')
output('UnicodeLib.UnicodeExtract50(const unicode src, unsigned4 instance)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeExtract50(const unicode src, unsigned4 instance)'); 
#END

#IF(UnicodeLib.UnicodeToLowerCase(F2) = U'abcde')
output('UnicodeLib.UnicodeToLowerCase(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeToLowerCase(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeToUpperCase(F7) = U'ABCDE')
output('UnicodeLib.UnicodeToUpperCase(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeToUpperCase(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeToProperCase(F7) = U'Abcde')
output('UnicodeLib.UnicodeToProperCase(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeToProperCase(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeToLowerCase80(F2) = U'abcde')
output('UnicodeLib.UnicodeToLowerCase80(const unicode src)');
#ELSE
output('FAILED UnicodeLib.UnicodeToLowerCase80(const unicode src)');
#END

#IF(UnicodeLib.UnicodeToUpperCase80(F7) = U'ABCDE')
output('UnicodeLib.UnicodeToUpperCase80(const unicode src)');
#ELSE
output('FAILED UnicodeLib.UnicodeToUpperCase80(const unicode src)');
#END

#IF(UnicodeLib.UnicodeToProperCase80(F7) = U'Abcde')
output('UnicodeLib.UnicodeToProperCase80(const unicode src)');
#ELSE
output('FAILED UnicodeLib.UnicodeToProperCase80(const unicode src)');
#END
/*
#IF(UnicodeLib.UnicodeLocaleToLowerCase(F2, V'en_us' = U'abcde')
output('UnicodeLib.UnicodeLocaleToLowerCase(const unicode src, const varstring localename)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToLowerCase(const unicode src, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleToUpperCase(F7, V'en_us' = U'ABCDE')
output('UnicodeLib.UnicodeLocaleToUpperCase(const unicode src, const varstring localename)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToUpperCase(const unicode src, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleToProperCase(F7, V'en_us' = U'Abcde ')
output('UnicodeLib.UnicodeLocaleToProperCase(const unicode src, const varstring localename)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToProperCase(const unicode src, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleToLowerCase80(F2, V'en_us' = U'abcde')
output('UnicodeLib.UnicodeLocaleToLowerCase80(const unicode src, const varstring localename)');
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToLowerCase80(const unicode src, const varstring localename)');
#END

#IF(UnicodeLib.UnicodeLocaleToUpperCase80(F7, V'en_us' = U'ABCDE')
output('UnicodeLib.UnicodeLocaleToUpperCase80(const unicode src, const varstring localename)');
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToUpperCase80(const unicode src, const varstring localename)');
#END

#IF(UnicodeLib.UnicodeLocaleToProperCase80(F7, V'en_us' = U'Abcde')
output('UnicodeLib.UnicodeLocaleToProperCase80(const unicode src, const varstring localename)');
#ELSE
output('FAILED UnicodeLib.UnicodeLocaleToProperCase80(const unicode src, const varstring localename)');
#END
*/
#IF(UnicodeLib.UnicodeCompareIgnoreCase(F2, F7) = 0)
output('UnicodeLib.UnicodeCompareIgnoreCase(const unicode src1, const unicode src2)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeCompareIgnoreCase(const unicode src1, const unicode src2)'); 
#END

#IF(UnicodeLib.UnicodeCompareAtStrength(F2, F7, 1) = 0)
output('UnicodeLib.UnicodeCompareAtStrength(const unicode src1, const unicode src2, integer1 strength)'); 
#ELSE
output('UnicodeLib.FAILED UnicodeCompareAtStrength(const unicode src1, const unicode src2, integer1 strength)'); 
#END
/*
#IF(UnicodeLib.UnicodeLocaleCompareIgnoreCase(F2, F7, V'en_us' = 0)
output('UnicodeLib.UnicodeLocaleCompareIgnoreCase(const unicode src1, const unicode src2, const varstring localename)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleCompareIgnoreCase(FAILED const unicode src1, const unicode src2, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleCompareAtStrength(F2, F7, V'en_us', 1) = 0)
output('UnicodeLib.UnicodeLocaleCompareAtStrength(const unicode src1, const unicode src2, const varstring localename, integer1 strength)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleCompareAtStrength(FAILED const unicode src1, const unicode src2, const varstring localename, integer1 strength)'); 
#END
*/
#IF(UnicodeLib.UnicodeReverse(F2) = U'EDCBA')
output('UnicodeLib.UnicodeReverse(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeReverse(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeFindReplace(F1, F4, F5 + F5) = U'AXXDEAXXDE')
output('UnicodeLib.UnicodeFindReplace(const unicode src, const unicode stok, const unicode rtok)'); 
#ELSE
output('UnicodeLibFAILED .UnicodeFindReplace(const unicode src, const unicode stok, const unicode rtok)'); 
#END
/*
#IF(UnicodeLib.UnicodeLocaleFindReplace(F1, F4, F5 + F5, V'en_us' = U'AXXDEAXXDE')
output('UnicodeLib.UnicodeLocaleFindReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleFindReplace(FAILED const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleFindAtStrengthReplace(F1, F4, F5 + F5, V'en_us', 1) = U'AXXDEAXXDE')
output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength)');
#ELSE
output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace(const unicode src, FAILED const unicode stok, const unicode rtok, const varstring localename, integer1 strength)');
#END
*/
#IF(UnicodeLib.UnicodeFindReplace80(F1, F4, F5 + F5) = U'AXXDEAXXDE')
output('UnicodeLib.UnicodeFindReplace80(const unicode src, const unicode stok, const unicode rtok)'); 
#ELSE
output('UnicodeLib.FAILED UnicodeFindReplace80(const unicode src, const unicode stok, const unicode rtok)'); 
#END
/*
#IF(UnicodeLib.UnicodeLocaleFindReplace80(F1, F4, F5 + F5, V'en_us' = U'AXXDEAXXDE')
output('UnicodeLib.UnicodeLocaleFindReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleFindReplace80(FAILED const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
#END

#IF(UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(F1, F4, F5 + F5, V'en_us', 1) = U'AXXDEAXXDE ')
output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength)'); 
#ELSE
output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(const unicode src, FAILED const unicode stok, const unicode rtok, const varstring localename, integer1 strength)'); 
#END
*/
#IF(UnicodeLib.UnicodeCleanSpaces(U'ABCDE    ABCDE   ABCDE') = U'ABCDE ABCDE ABCDE')
output('UnicodeLib.UnicodeCleanSpaces(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeCleanSpaces(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeCleanSpaces25(U'ABCDE    ABCDE   ABCDE') = U'ABCDE ABCDE ABCDE')
output('UnicodeLib.UnicodeCleanSpaces25(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeCleanSpaces25(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeCleanSpaces80(U'ABCDE    ABCDE   ABCDE') = U'ABCDE ABCDE ABCDE')
output('UnicodeLib.UnicodeCleanSpaces80(const unicode src)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeCleanSpaces80(const unicode src)'); 
#END

#IF(UnicodeLib.UnicodeWildMatch(U'ABCDE', U'a?c*', true) = true)
output('UnicodeLib.UnicodeWildMatch(const unicode src, const unicode _pattern, boolean _noCase)');          
#ELSE
output('FAILED UnicodeLib.UnicodeWildMatch(const unicode src, const unicode _pattern, boolean _noCase)');           
#END

#IF(UnicodeLib.UnicodeContains(F2, U'abc', true) = true)
output('UnicodeLib.UnicodeContains(const unicode src, const unicode _pattern, boolean _noCase)'); 
#ELSE
output('FAILED UnicodeLib.UnicodeContains(const unicode src, const unicode _pattern, boolean _noCase)'); 
#END

/**/