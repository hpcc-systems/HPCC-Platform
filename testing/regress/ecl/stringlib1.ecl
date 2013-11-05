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

import Std.Str;
import lib_stringlib;

//TRANSFORM function usage -- THOR

InRec := RECORD
  STRING10          F1;     //'ABCDEABCDE'
  EBCDIC STRING10   F1_E;       //'ABCDEABCDE'
  STRING5           F2;     //'ABCDE'
  EBCDIC STRING5    F2_E;   //'ABCDE'
  STRING2           F3;     //'BD'
  STRING2           F4;     //'BC'
  EBCDIC STRING2    F4_E;   //'BC'
  STRING1           F5;     //'X'
  STRING7           F6;     //'AB,CD,E'
  STRING5           F7;     //'abcde'
  DATA2             CRLF;   // := x'0a0d';
 
END;

InData := DATASET([
{ 'ABCDEABCDE',
  'ABCDEABCDE',
  'ABCDE',
  'ABCDE',
  'BD',
  'BC',
  'BC',
  'X',
  'AB,CD,E',
  'abcde',
  x'0a0d'
},
{ 'ABCDEABCDE',
  'ABCDEABCDE',
  'ABCDE',
  'ABCDE',
  'BD',
  'BC',
  'BC',
  'X',
  'AB,CD,E',
  'abcde',
  x'0a0d'
}
],InRec);

OutRec := RECORD
  STRING  StringFilterOut;                          
  STRING  StringFilter;                                 
  STRING  StringSubstituteOut;  
  STRING  StringSubstitute;     
  STRING  StringRepad_1;                                        
  STRING  StringRepad_2;                                        
  INTEGER StringFind_1;                 
  INTEGER StringFind_2;                 
  INTEGER StringFind_3;                 
  INTEGER EbcdicStringFind_1; 
  INTEGER EbcdicStringFind_2; 
  INTEGER EbcdicStringFind_3; 
  INTEGER StringFind2;                              
  INTEGER EbcdicStringFind2;            
  STRING  StringExtract;                                
  BOOLEAN GetDateYYYYMMDD;                                                                  
  BOOLEAN GetBuildInfo;                                                                         
  STRING  Data2String;                                                      
  STRING  String2Data;                                                      
  STRING  StringToLowerCase;                                                
  STRING  StringToUpperCase;                                                
  STRING  StringToProperCase;
  INTEGER StringCompareIgnoreCase_1;
  INTEGER StringCompareIgnoreCase_2;
  INTEGER StringCompareIgnoreCase_3;
  STRING  StringReverse;
  STRING  StringFindReplace;
  STRING  StringCleanSpaces;
  BOOLEAN StringWildMatch_1;
  BOOLEAN StringWildMatch_2;
  BOOLEAN StringContains_1;
  BOOLEAN StringContains_2;
  BOOLEAN StringContains_3;
  BOOLEAN StringContains_4;
  STRING  StringExtractMultiple_1;
  STRING  StringExtractMultiple_2;
  STRING  StringExtractMultiple_3;
  STRING  StringExtractMultiple_4;
  STRING  StringExtractMultiple_5;
  STRING  StringExtractMultiple_6;
  STRING  StringExtractMultiple_7;
END;

Str.ExtractMultiple('1,2,345,,5', 0b0001);  //'1';
Str.ExtractMultiple('1,2,345,,5', 0b0011);  //'1,2';
Str.ExtractMultiple('1,2,345,,5', 0b0101);  //'1,345';
Str.ExtractMultiple('1,2,345,,5', 0b1000);  //'';
Str.ExtractMultiple('1,2,345,,5', 0b11111111111111111);  //'1,2,345,,5';
Str.ExtractMultiple('1,2,345,,5', 0b0);  //'';
Str.ExtractMultiple('1,2,345,,5', 0b10000000000000000);  //'';


OutRec Xform(InRec L) := TRANSFORM
  SELF.StringFilterOut             := Str.FilterOut(L.F2, L.F3);    //ACE               
  SELF.StringFilter                := Str.Filter(L.F2, L.F3);       //BD                
  SELF.StringSubstituteOut         := Str.SubstituteIncluded(L.F2, L.F3, L.F5); //AXCXE  
  SELF.StringSubstitute            := Str.SubstituteExcluded(L.F2, L.F3, L.F5);     //XBXDX     
  SELF.StringRepad_1               := lib_stringlib.StringLib.StringRepad('ABCDE   ', 6);   //'ABCDE '                      
  SELF.StringRepad_2               := lib_stringlib.StringLib.StringRepad('ABCDE   ', 3);   //'ABC'                                             
  SELF.StringFind_1                := Str.Find(L.F2, L.F4, 1 );         //2         
  SELF.StringFind_2                := Str.Find(L.F2, L.F4, 2 );         //0         
  SELF.StringFind_3                := Str.Find(L.F1, L.F4, 2 ); //7                 
  SELF.EbcdicStringFind_1          := lib_stringlib.StringLib.EbcdicStringFind(L.F2_E, L.F4_E, 1 );         //2         
  SELF.EbcdicStringFind_2          := lib_stringlib.StringLib.EbcdicStringFind(L.F2_E, L.F4_E, 2 );         //0         
  SELF.EbcdicStringFind_3          := lib_stringlib.StringLib.EbcdicStringFind(L.F1_E, L.F4_E, 2 ); //7                 
  SELF.StringFind2                 := lib_stringlib.StringLib.StringUnboundedUnsafeFind(L.F2, L.F4);        //2                     
  SELF.EbcdicStringFind2           := lib_stringlib.StringLib.EbcdicStringUnboundedUnsafeFind(L.F2_E, L.F4_E);      //2         
  SELF.StringExtract               := Str.Extract(L.F6, 2);     //CD                                            
  SELF.GetDateYYYYMMDD             := length(StringLib.GetDateYYYYMMDD()) = 8;  //true
  SELF.GetBuildInfo                := StringLib.GetBuildInfo() <> '';       //true
  SELF.Data2String                 := StringLib.Data2String(L.CRLF);    //0A0D                                                  
  SELF.String2Data                 := (>string<)StringLib.String2Data('4142');  //AB                                                            
  SELF.StringToLowerCase           := Str.ToLowerCase(L.F2);    //abcde                                                                         
  SELF.StringToUpperCase           := Str.ToUpperCase(L.F7);    //ABCDE                                                 
  SELF.StringToProperCase          := Str.ToCapitalCase(L.F7);  //Abcde                                 
  SELF.StringCompareIgnoreCase_1   := Str.CompareIgnoreCase(L.F2,L.F2); //0                         
  SELF.StringCompareIgnoreCase_2   := Str.CompareIgnoreCase(L.F2,'abcda'); //1                          
  SELF.StringCompareIgnoreCase_3   := Str.CompareIgnoreCase(L.F2,'ABCDF'); //-1                         
  SELF.StringReverse               := Str.Reverse(L.F2); //EDCBA                                            
  SELF.StringFindReplace           := Str.FindReplace(L.F1, L.F4, 'XY');    //AXYDEAXYDE
  SELF.StringCleanSpaces           := Str.CleanSpaces('ABCDE    ABCDE   ABCDE');    //ABCDE ABCDE ABCDE
  SELF.StringWildMatch_1           := Str.WildMatch(L.F2, 'a?c*', true);    //true      
  SELF.StringWildMatch_2           := Str.WildMatch(L.F2, 'a?c*', false);   //false     
  SELF.StringContains_1            := Str.Contains(L.F2, 'abc', true);  //true          
  SELF.StringContains_2            := Str.Contains(L.F2, 'abc', false); //false             
  SELF.StringContains_3            := Str.Contains(L.F2, 'def', true);  //false         
  SELF.StringContains_4            := Str.Contains(L.F2, 'def', false); //false 

  SELF.StringExtractMultiple_1     := Str.ExtractMultiple('1,2,345,,5', 0b0001);  //'1';
  SELF.StringExtractMultiple_2     := Str.ExtractMultiple('1,2,345,,5', 0b0011);  //'1,2';
  SELF.StringExtractMultiple_3     := Str.ExtractMultiple('1,2,345,,5', 0b0101);  //'1,345';
  SELF.StringExtractMultiple_4     := Str.ExtractMultiple('1,2,345,,5', 0b1000);  //'';
  SELF.StringExtractMultiple_5     := Str.ExtractMultiple('1,2,345,,5', 0b11111111111111111);  //'1,2,345,,5';
  SELF.StringExtractMultiple_6     := Str.ExtractMultiple('1,2,345,,5', 0b0);  //'';
  SELF.StringExtractMultiple_7     := Str.ExtractMultiple('1,2,345,,5', 0b10000000000000000);  //'';
                                   
END;

output(PROJECT(InData,Xform(LEFT)));
