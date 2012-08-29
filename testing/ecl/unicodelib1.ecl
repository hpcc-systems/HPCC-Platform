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

//TRANSFORM function usage -- THOR


InRec := RECORD
  UNICODE10     F1;//    := U'ABCDEABCDE';
  UNICODE5      F2;//    := U'ABCDE';
  UNICODE2      F3;//    := U'BD';
  UNICODE2      F4;//    := U'BC';
  UNICODE1      F5;//    := U'X';
  UNICODE7      F6;//    := U'AB,CD,E';
  UNICODE5      F7;//    := U'abcde';
END;

InData := DATASET([
{
  U'ABCDEABCDE',
  U'ABCDE',
  U'BD',
  U'BC',
  U'X',
  U'AB,CD,E',
  U'abcde'
},
{
  U'ABCDEABCDE',
  U'ABCDE',
  U'BD',
  U'BC',
  U'X',
  U'AB,CD,E',
  U'abcde'
}
],InRec);

OutRec := RECORD
  unicode   UnicodeFilterOut;
  unicode   UnicodeFilter;
  unicode   UnicodeSubstituteOut;
  unicode   UnicodeSubstitute;
  unicode   UnicodeRepad_1;
  unicode   UnicodeRepad_2;
  unsigned4 UnicodeFind_1;
  unsigned4 UnicodeFind_2;
  unsigned4 UnicodeFind_3;
  unsigned4 UnicodeLocaleFind_1;
  unsigned4 UnicodeLocaleFind_2;
  unsigned4 UnicodeLocaleFind_3;
  unsigned4 UnicodeLocaleFindAtStrength_1;
  unsigned4 UnicodeLocaleFindAtStrength_2;
  unsigned4 UnicodeLocaleFindAtStrength_3;
  unicode   UnicodeExtract;
  unicode50 UnicodeExtract50;
  unicode   UnicodeToLowerCase;
  unicode   UnicodeToUpperCase;
  unicode   UnicodeToProperCase;
  unicode80 UnicodeToLowerCase80;
  unicode80 UnicodeToUpperCase80;
  unicode80 UnicodeToProperCase80;
  unicode   UnicodeLocaleToLowerCase;
  unicode   UnicodeLocaleToUpperCase;
  unicode   UnicodeLocaleToProperCase;
  unicode80 UnicodeLocaleToLowerCase80;
  unicode80 UnicodeLocaleToUpperCase80;
  unicode80 UnicodeLocaleToProperCase80;
  integer4  UnicodeCompareIgnoreCase_1;
  integer4  UnicodeCompareIgnoreCase_2;
  integer4  UnicodeCompareIgnoreCase_3;
  integer4  UnicodeCompareAtStrength_1;
  integer4  UnicodeCompareAtStrength_2;
  integer4  UnicodeCompareAtStrength_3;
  integer4  UnicodeLocaleCompareIgnoreCase_1;
  integer4  UnicodeLocaleCompareIgnoreCase_2;
  integer4  UnicodeLocaleCompareIgnoreCase_3;
  integer4  UnicodeLocaleCompareAtStrength_1;
  integer4  UnicodeLocaleCompareAtStrength_2;
  integer4  UnicodeLocaleCompareAtStrength_3;
  unicode   UnicodeReverse;
  unicode   UnicodeFindReplace;
  unicode   UnicodeLocaleFindReplace;
  unicode   UnicodeLocaleFindAtStrengthReplace;
  unicode80 UnicodeFindReplace80;
  unicode80 UnicodeLocaleFindReplace80;
  unicode80 UnicodeLocaleFindAtStrengthReplace80;
  unicode   UnicodeCleanSpaces;
  unicode25 UnicodeCleanSpaces25;
  unicode80 UnicodeCleanSpaces80;
  boolean   UnicodeContains_1;
  boolean   UnicodeContains_2;
  boolean   UnicodeContains_3;
  boolean   UnicodeContains_4;
END;

OutRec Xform(InRec L) := TRANSFORM
  SELF.UnicodeFilterOut                        := UnicodeLib.UnicodeFilterOut(L.F2, L.F3); //ACE
  SELF.UnicodeFilter                           := UnicodeLib.UnicodeFilter(L.F2, L.F3);     //BD
  SELF.UnicodeSubstituteOut                    := UnicodeLib.UnicodeSubstituteOut(L.F2, L.F3, L.F5); //AXCXE
  SELF.UnicodeSubstitute                       := UnicodeLib.UnicodeSubstitute(L.F2, L.F3, L.F5); //XBXDX
  SELF.UnicodeRepad_1                          := UnicodeLib.UnicodeRepad(U'ABCDE   ', 6); //'ABCDE '
  SELF.UnicodeRepad_2                          := UnicodeLib.UnicodeRepad(U'ABCDE   ', 3); //'ABC'
  SELF.UnicodeFind_1                           := UnicodeLib.UnicodeFind(L.F2, L.F4, 1); //2
  SELF.UnicodeFind_2                           := UnicodeLib.UnicodeFind(L.F2, L.F4, 2); //0
  SELF.UnicodeFind_3                           := UnicodeLib.UnicodeFind(L.F1, L.F4, 2); //7
  SELF.UnicodeLocaleFind_1                     := UnicodeLib.UnicodeLocaleFind(L.F2, L.F4, 1, V'en_us'); //2
  SELF.UnicodeLocaleFind_2                     := UnicodeLib.UnicodeLocaleFind(L.F2, L.F4, 2, V'en_us'); //0
  SELF.UnicodeLocaleFind_3                     := UnicodeLib.UnicodeLocaleFind(L.F1, L.F4, 2, V'en_us'); //7
  SELF.UnicodeLocaleFindAtStrength_1           := UnicodeLib.UnicodeLocaleFindAtStrength(L.F2, L.F4, 1, V'en_us', 1); //2
  SELF.UnicodeLocaleFindAtStrength_2           := UnicodeLib.UnicodeLocaleFindAtStrength(L.F2, L.F4, 2, V'en_us', 1); //0
  SELF.UnicodeLocaleFindAtStrength_3           := UnicodeLib.UnicodeLocaleFindAtStrength(L.F1, L.F4, 2, V'en_us', 1); //7
  SELF.UnicodeExtract                          := UnicodeLib.UnicodeExtract(L.F6, 2);   //CD
  SELF.UnicodeExtract50                        := UnicodeLib.UnicodeExtract50(L.F6, 2); //CD
  SELF.UnicodeToLowerCase                      := UnicodeLib.UnicodeToLowerCase(L.F2); //abcde
  SELF.UnicodeToUpperCase                      := UnicodeLib.UnicodeToUpperCase(L.F7); //ABCDE
  SELF.UnicodeToProperCase                     := UnicodeLib.UnicodeToProperCase(L.F7); //Abcde
  SELF.UnicodeToLowerCase80                    := UnicodeLib.UnicodeToLowerCase80(L.F2); //abcde
  SELF.UnicodeToUpperCase80                    := UnicodeLib.UnicodeToUpperCase80(L.F7); //ABCDE
  SELF.UnicodeToProperCase80                   := UnicodeLib.UnicodeToProperCase80(L.F7); //Abcde
  SELF.UnicodeLocaleToLowerCase                := UnicodeLib.UnicodeLocaleToLowerCase(L.F2, V'en_us'); //abcde
  SELF.UnicodeLocaleToUpperCase                := UnicodeLib.UnicodeLocaleToUpperCase(L.F7, V'en_us'); //ABCDE
  SELF.UnicodeLocaleToProperCase               := UnicodeLib.UnicodeLocaleToProperCase(L.F7, V'en_us'); //Abcde 
  SELF.UnicodeLocaleToLowerCase80              := UnicodeLib.UnicodeLocaleToLowerCase80(L.F2, V'en_us'); //abcde
  SELF.UnicodeLocaleToUpperCase80              := UnicodeLib.UnicodeLocaleToUpperCase80(L.F7, V'en_us'); //ABCDE
  SELF.UnicodeLocaleToProperCase80             := UnicodeLib.UnicodeLocaleToProperCase80(L.F7, V'en_us'); //Abcde
  SELF.UnicodeCompareIgnoreCase_1              := UnicodeLib.UnicodeCompareIgnoreCase(L.F2, L.F7); //0
  SELF.UnicodeCompareIgnoreCase_2              := UnicodeLib.UnicodeCompareIgnoreCase(L.F2, L.F1); //1
  SELF.UnicodeCompareIgnoreCase_3              := UnicodeLib.UnicodeCompareIgnoreCase(L.F3, L.F2); //-1
  SELF.UnicodeCompareAtStrength_1              := UnicodeLib.UnicodeCompareAtStrength(L.F2, L.F7, 1); //0
  SELF.UnicodeCompareAtStrength_2              := UnicodeLib.UnicodeCompareAtStrength(L.F2, L.F1, 1); //-1
  SELF.UnicodeCompareAtStrength_3              := UnicodeLib.UnicodeCompareAtStrength(L.F3, L.F2, 1); //1
  SELF.UnicodeLocaleCompareIgnoreCase_1        := UnicodeLib.UnicodeLocaleCompareIgnoreCase(L.F2, L.F7, V'en_us'); //0
  SELF.UnicodeLocaleCompareIgnoreCase_2        := UnicodeLib.UnicodeLocaleCompareIgnoreCase(L.F2, L.F1, V'en_us'); //-1
  SELF.UnicodeLocaleCompareIgnoreCase_3        := UnicodeLib.UnicodeLocaleCompareIgnoreCase(L.F3, L.F2, V'en_us'); //1
  SELF.UnicodeLocaleCompareAtStrength_1        := UnicodeLib.UnicodeLocaleCompareAtStrength(L.F2, L.F7, V'en_us', 1); //0
  SELF.UnicodeLocaleCompareAtStrength_2        := UnicodeLib.UnicodeLocaleCompareAtStrength(L.F2, L.F1, V'en_us', 1); //-1
  SELF.UnicodeLocaleCompareAtStrength_3        := UnicodeLib.UnicodeLocaleCompareAtStrength(L.F3, L.F2, V'en_us', 1); //1
  SELF.UnicodeReverse                          := UnicodeLib.UnicodeReverse(L.F2); //EDCBA
  SELF.UnicodeFindReplace                      := UnicodeLib.UnicodeFindReplace(L.F1, L.F4, L.F5 + L.F5);  //AXXDEAXXDE
  SELF.UnicodeLocaleFindReplace                := UnicodeLib.UnicodeLocaleFindReplace(L.F1, L.F4, L.F5 + L.F5, V'en_us');   //AXXDEAXXDE
  SELF.UnicodeLocaleFindAtStrengthReplace      := UnicodeLib.UnicodeLocaleFindAtStrengthReplace(L.F1, L.F4, L.F5 + L.F5, V'en_us', 1); //AXXDEAXXDE
  SELF.UnicodeFindReplace80                    := UnicodeLib.UnicodeFindReplace80(L.F1, L.F4, L.F5 + L.F5); //AXXDEAXXDE
  SELF.UnicodeLocaleFindReplace80              := UnicodeLib.UnicodeLocaleFindReplace80(L.F1, L.F4, L.F5 + L.F5, V'en_us'); //AXXDEAXXDE
  SELF.UnicodeLocaleFindAtStrengthReplace80    := UnicodeLib.UnicodeLocaleFindAtStrengthReplace80(L.F1, L.F4, L.F5 + L.F5, V'en_us', 1); //AXXDEAXXDE 
  SELF.UnicodeCleanSpaces                      := UnicodeLib.UnicodeCleanSpaces(U'ABCDE    ABCDE   ABCDE'); //ABCDE ABCDE ABCDE
  SELF.UnicodeCleanSpaces25                    := UnicodeLib.UnicodeCleanSpaces25(U'ABCDE    ABCDE   ABCDE'); //ABCDE ABCDE ABCDE
  SELF.UnicodeCleanSpaces80                    := UnicodeLib.UnicodeCleanSpaces80(U'ABCDE    ABCDE   ABCDE'); //ABCDE ABCDE ABCDE
  SELF.UnicodeContains_1                       := UnicodeLib.UnicodeContains(L.F2, U'abc', true);  //true
  SELF.UnicodeContains_2                       := UnicodeLib.UnicodeContains(L.F2, U'abc', false); //false 
  SELF.UnicodeContains_3                       := UnicodeLib.UnicodeContains(L.F2, U'def', true);  //false
  SELF.UnicodeContains_4                       := UnicodeLib.UnicodeContains(L.F2, U'def', false); //false 
END;

output(PROJECT(InData,Xform(LEFT)));



