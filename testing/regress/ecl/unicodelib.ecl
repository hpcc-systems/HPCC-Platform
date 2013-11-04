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

//Straight libary function usage -- hthor

UNICODE10       F1   := U'ABCDEABCDE';
UNICODE5        F2   := U'ABCDE';
UNICODE2        F3   := U'BD';
UNICODE2        F4   := U'BC';
UNICODE1        F5   := U'X';
UNICODE7        F6   := U'AB,CD,E';
UNICODE5        F7   := U'abcde';
DATA2           CRLF := x'0a0d';

output('UnicodeLib.UnicodeFilterOut(const unicode src, const unicode _within)'); 
OUTPUT(UnicodeLib.UnicodeFilterOut(F2, F3));  // U'ACE')

output('UnicodeLib.UnicodeFilter(const unicode src, const unicode _within)'); 
OUTPUT(UnicodeLib.UnicodeFilter(F2, F3));  // U'BD')

output('UnicodeLib.UnicodeSubstituteOut(const unicode src, const unicode _within, const unicode _newchar)'); 
OUTPUT(UnicodeLib.UnicodeSubstituteOut(F2, F3, F5));  // U'AXCXE')

output('UnicodeLib.UnicodeSubstitute(const unicode src, const unicode _within, const unicode _newchar)'); 
OUTPUT(UnicodeLib.UnicodeSubstitute(F2, F3, F5));  // U'XBXDX')

output('UnicodeLib.UnicodeRepad(const unicode src, unsigned4 size)'); 
output(UnicodeLib.UnicodeRepad(U'ABCDE   ', 6)); //'ABCDE '
output(UnicodeLib.UnicodeRepad(U'ABCDE   ', 3)); //'ABC'

output('UnicodeLib.UnicodeFind(const unicode src, const unicode tofind, unsigned4 instance)');
output(UnicodeLib.UnicodeFind(F2, F4, 1)); //2
output(UnicodeLib.UnicodeFind(F2, F4, 2)); //0
output(UnicodeLib.UnicodeFind(F1, F4, 2)); //7

output('UnicodeLib.UnicodeLocaleFind(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename)'); 
output(UnicodeLib.UnicodeLocaleFind(F2, F4, 1, V'en_us')); //2
output(UnicodeLib.UnicodeLocaleFind(F2, F4, 2, V'en_us')); //0
output(UnicodeLib.UnicodeLocaleFind(F1, F4, 2, V'en_us')); //7

output('UnicodeLib.UnicodeLocaleFindAtStrength(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename, integer1 strength)'); 
output(UnicodeLib.UnicodeLocaleFindAtStrength(F2, F4, 1, V'en_us', 1)); //2
output(UnicodeLib.UnicodeLocaleFindAtStrength(F2, F4, 2, V'en_us', 1)); //0
output(UnicodeLib.UnicodeLocaleFindAtStrength(F1, F4, 2, V'en_us', 1)); //7

output('UnicodeLib.UnicodeExtract(const unicode src, unsigned4 instance)'); 
OUTPUT(UnicodeLib.UnicodeExtract(F6, 2));  // U'CD')

output('UnicodeLib.UnicodeExtract50(const unicode src, unsigned4 instance)'); 
OUTPUT(UnicodeLib.UnicodeExtract50(F6, 2));  // U'CD')

output('UnicodeLib.UnicodeToLowerCase(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeToLowerCase(F2));  // U'abcde')

output('UnicodeLib.UnicodeToUpperCase(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeToUpperCase(F7));  // U'ABCDE')

output('UnicodeLib.UnicodeToProperCase(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeToProperCase(F7));  // U'Abcde')

output('UnicodeLib.UnicodeToLowerCase80(const unicode src)');
OUTPUT(UnicodeLib.UnicodeToLowerCase80(F2));  // U'abcde')

output('UnicodeLib.UnicodeToUpperCase80(const unicode src)');
OUTPUT(UnicodeLib.UnicodeToUpperCase80(F7));  // U'ABCDE')

output('UnicodeLib.UnicodeToProperCase80(const unicode src)');
OUTPUT(UnicodeLib.UnicodeToProperCase80(F7));  // U'Abcde')

output('UnicodeLib.UnicodeLocaleToLowerCase(const unicode src, const varstring localename)'); 
OUTPUT(UnicodeLib.UnicodeLocaleToLowerCase(F2, V'en_us'));  // U'abcde')

output('UnicodeLib.UnicodeLocaleToUpperCase(const unicode src, const varstring localename)'); 
OUTPUT(UnicodeLib.UnicodeLocaleToUpperCase(F7, V'en_us'));  // U'ABCDE')

output('UnicodeLib.UnicodeLocaleToProperCase(const unicode src, const varstring localename)'); 
OUTPUT(UnicodeLib.UnicodeLocaleToProperCase(F7, V'en_us'));  // U'Abcde ')

output('UnicodeLib.UnicodeLocaleToLowerCase80(const unicode src, const varstring localename)');
OUTPUT(UnicodeLib.UnicodeLocaleToLowerCase80(F2, V'en_us'));  // U'abcde')

output('UnicodeLib.UnicodeLocaleToUpperCase80(const unicode src, const varstring localename)');
OUTPUT(UnicodeLib.UnicodeLocaleToUpperCase80(F7, V'en_us'));  // U'ABCDE')

output('UnicodeLib.UnicodeLocaleToProperCase80(const unicode src, const varstring localename)');
OUTPUT(UnicodeLib.UnicodeLocaleToProperCase80(F7, V'en_us'));  // U'Abcde')

output('UnicodeLib.UnicodeCompareIgnoreCase(const unicode src1, const unicode src2)'); 
output(UnicodeLib.UnicodeCompareIgnoreCase(F2, F7)); //0
output(UnicodeLib.UnicodeCompareIgnoreCase(F2, F1)); //1
output(UnicodeLib.UnicodeCompareIgnoreCase(F3, F2)); //-1

output('UnicodeLib.UnicodeCompareAtStrength(const unicode src1, const unicode src2, integer1 strength)'); 
output(UnicodeLib.UnicodeCompareAtStrength(F2, F7, 1)); //0
output(UnicodeLib.UnicodeCompareAtStrength(F2, F1, 1)); //-1
output(UnicodeLib.UnicodeCompareAtStrength(F3, F2, 1)); //1

output('UnicodeLib.UnicodeLocaleCompareIgnoreCase(const unicode src1, const unicode src2, const varstring localename)'); 
output(UnicodeLib.UnicodeLocaleCompareIgnoreCase(F2, F7, V'en_us')); //0
output(UnicodeLib.UnicodeLocaleCompareIgnoreCase(F2, F1, V'en_us')); //-1
output(UnicodeLib.UnicodeLocaleCompareIgnoreCase(F3, F2, V'en_us')); //1

output('UnicodeLib.UnicodeLocaleCompareAtStrength(const unicode src1, const unicode src2, const varstring localename, integer1 strength)'); 
output(UnicodeLib.UnicodeLocaleCompareAtStrength(F2, F7, V'en_us', 1)); //0
output(UnicodeLib.UnicodeLocaleCompareAtStrength(F2, F1, V'en_us', 1)); //-1
output(UnicodeLib.UnicodeLocaleCompareAtStrength(F3, F2, V'en_us', 1)); //1

output('UnicodeLib.UnicodeReverse(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeReverse(F2));  // U'EDCBA')

output('UnicodeLib.UnicodeFindReplace(const unicode src, const unicode stok, const unicode rtok)'); 
OUTPUT(UnicodeLib.UnicodeFindReplace(F1, F4, F5 + F5));  // U'AXXDEAXXDE')

output('UnicodeLib.UnicodeLocaleFindReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
OUTPUT(UnicodeLib.UnicodeLocaleFindReplace(F1, F4, F5 + F5, V'en_us'));  // U'AXXDEAXXDE')

output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength)');
OUTPUT(UnicodeLib.UnicodeLocaleFindAtStrengthReplace(F1, F4, F5 + F5, V'en_us', 1));  // U'AXXDEAXXDE')

output('UnicodeLib.UnicodeFindReplace80(const unicode src, const unicode stok, const unicode rtok)'); 
OUTPUT(UnicodeLib.UnicodeFindReplace80(F1, F4, F5 + F5));  // U'AXXDEAXXDE')

output('UnicodeLib.UnicodeLocaleFindReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename)'); 
OUTPUT(UnicodeLib.UnicodeLocaleFindReplace80(F1, F4, F5 + F5, V'en_us'));  // U'AXXDEAXXDE')

output('UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength)'); 
OUTPUT(UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(F1, F4, F5 + F5, V'en_us', 1));  // U'AXXDEAXXDE ')

output('UnicodeLib.UnicodeCleanSpaces(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeCleanSpaces(U'ABCDE    ABCDE   ABCDE'));  // U'ABCDE ABCDE ABCDE')

output('UnicodeLib.UnicodeCleanSpaces25(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeCleanSpaces25(U'ABCDE    ABCDE   ABCDE'));  // U'ABCDE ABCDE ABCDE')

output('UnicodeLib.UnicodeCleanSpaces80(const unicode src)'); 
OUTPUT(UnicodeLib.UnicodeCleanSpaces80(U'ABCDE    ABCDE   ABCDE'));  // U'ABCDE ABCDE ABCDE')

output('UnicodeLib.UnicodeWildMatch(const unicode src, const unicode _pattern, boolean _noCase)');          
output(UnicodeLib.UnicodeWildMatch(U'ABCDE', U'a?c*', true));   //true      
output(UnicodeLib.UnicodeWildMatch(U'ABCDE', U'a?c*', false));  //false     
output(UnicodeLib.UnicodeWildMatch(U'stra\u00DFe', U'*a?e', false));    //true
output(UnicodeLib.UnicodeWildMatch(U'stra\u00DFe', U'*A?E', true));     //true, possibly incorrectly: sharp s should ucase to SS (see docs)
output(UnicodeLib.UnicodeWildMatch(U'stra\u00DFe', U'*A??E', true));    //false, possibly incorrectly: sharp s should ucase to SS (see docs)
output(UnicodeLib.UnicodeWildMatch(UnicodeLib.UnicodeToUpperCase(U'stra\u00DFe'), U'*A?E', false));     //false, probably correctly: sharp s ucases to SS (see docs)
output(UnicodeLib.UnicodeWildMatch(UnicodeLib.UnicodeToUpperCase(U'stra\u00DFe'), U'*A??E', false));    //true, probably correctly: sharp s ucases to SS (see docs)

output('UnicodeLib.UnicodeContains(const unicode src, const unicode _pattern, boolean _noCase)'); 
output(UnicodeLib.UnicodeContains(F2, U'abc', true));  //true
output(UnicodeLib.UnicodeContains(F2, U'abc', false)); //false 
output(UnicodeLib.UnicodeContains(F2, U'def', true));  //false
output(UnicodeLib.UnicodeContains(F2, U'def', false)); //false 

output(UnicodeLib.UnicodeCleanAccents(U'Ú'));
output(U'Ú');