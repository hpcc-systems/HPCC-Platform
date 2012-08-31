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

import lib_stringlib;
import Std.Str;

//Straight libary function usage -- hthor

output('Str.FilterOut(string src, string _within)');                            
output(Str.FilterOut('ABCDE', 'BD'));   //ACE

output('Str.Filter(string src, string _within)');                               
output(Str.Filter('ABCDE', 'BD'));      //BD

output('Str.SubstituteIncluded(string src, string _within, string _newchar)');  
output(Str.SubstituteIncluded('ABCDE', 'BD', 'X')); //AXCXE  

output('Str.SubstituteExcluded(string src, string _within, string _newchar)');  
output(Str.SubstituteExcluded('ABCDE', 'BD', 'X'));     //XBXDX     

output('StringLib.StringRepad(string src, unsigned4 size)');                                        
output(lib_stringlib.StringLib.StringRepad('ABCDE   ', 6));     //'ABCDE '
output(lib_stringlib.StringLib.StringRepad('ABCDE   ', 3));     //'ABC'                     

output('Str.Find(string src, string tofind, unsigned4 instance )');                 
output(Str.Find('ABCDE', 'BC', 1 ));        //2         
output(Str.Find('ABCDE', 'BC', 2 ));        //0         
output(Str.Find('ABCDEABCDE', 'BC', 2 ));   //7                 

output('StringLib.EbcdicStringFind(ebcdic string src, ebcdic string tofind , unsigned4 instance )'); 
ebcdic string a := 'ABCDE';
ebcdic string b := 'BC';
ebcdic string c := 'ABCDEABCDE';
output(lib_stringlib.StringLib.EbcdicStringFind(a, b, 1 ));         //2         
output(lib_stringlib.StringLib.EbcdicStringFind(a, b, 2 ));         //0         
output(lib_stringlib.StringLib.EbcdicStringFind(c, b, 2 )); //7                 

output('StringLib.UnboundedUnsafeFind(string src, string tofind )');                                
output(lib_stringlib.StringLib.StringUnboundedUnsafeFind('ABCDE', 'BC'));       //2         

output('StringLib.EbcdicStringUnboundedUnsafeFind(ebcdic string src, ebcdic string tofind )');          
output(lib_stringlib.StringLib.EbcdicStringUnboundedUnsafeFind(a, b));      //2         

output('Str.Extract(string src, unsigned4 instance)');                              
output(Str.Extract('AB,CD,E', 2));  //CD                            

output('StringLib.GetDateYYYYMMDD()');                                                                  
output(length(StringLib.GetDateYYYYMMDD()) = 8);    //true                                                      

output('StringLib.GetBuildInfo()');                                                                         
output(StringLib.GetBuildInfo() <> '');         //current build info                                                                

output('StringLib.Data2String(data src)');                                                      
data2 CRLF := x'0a0d';
output(StringLib.Data2String(CRLF));    //0A0D

output('StringLib.String2Data(string src)');                                                        
output((>string<)StringLib.String2Data('4142'));    //AB

output('Str.ToLowerCase(string src)');                                              
output(Str.ToLowerCase('ABCDE'));   //abcde                                         

output('Str.ToUpperCase(string src)');                                              
output(Str.ToUpperCase('abcde'));   //ABCDE                                         

output('Str.ToCapitalCase(string src)');                                                
output(Str.ToCapitalCase('abcde')); //Abcde

output('Str.CompareIgnoreCase(string src1, string src2)');                          
output(Str.CompareIgnoreCase('ABCDE','abcde')); //0                         
output(Str.CompareIgnoreCase('ABCDE','abcda')); //1                         
output(Str.CompareIgnoreCase('abcde','ABCDF')); //-1                            

output('Str.Reverse(string src)');                                                  
output(Str.Reverse('ABCDE')); //EDCBA                                                   

output('Str.FindReplace(string src, string stok, string rtok)');            
output(Str.FindReplace('ABCDEABCDE', 'BC', 'XY'));  //AXYDEAXYDE

output('Str.CleanSpaces(string src)');                                              
output(Str.CleanSpaces('ABCDE    ABCDE   ABCDE'));  //ABCDE ABCDE ABCDE

output('Str.WildMatch(string src, string _pattern, boolean _noCase)');          
output(Str.WildMatch('ABCDE', 'a?c*', true));   //true      
output(Str.WildMatch('ABCDE', 'a?c*', false));  //false     

output('Str.Contains(string src, string _pattern, boolean _noCase)');           
output(Str.Contains('ABCDE', 'abc', true));  //true         
output(Str.Contains('ABCDE', 'abc', false)); //false            
output(Str.Contains('ABCDE', 'def', true));  //false            
output(Str.Contains('ABCDE', 'def', false)); //false    


output('Str.ExtractMultiple(commastring, bitmap)');
output(Str.ExtractMultiple('1,2,345,,5', 0b0001));  //'1';
output(Str.ExtractMultiple('1,2,345,,5', 0b0011));  //'1,2';
output(Str.ExtractMultiple('1,2,345,,5', 0b0101));  //'1,345';
output(Str.ExtractMultiple('1,2,345,,5', 0b1000));  //'';
output(Str.ExtractMultiple('1,2,345,,5', 0b11111111111111111));  //'1,2,345,,5';
output(Str.ExtractMultiple('1,2,345,,5', 0b0));  //'';
output(Str.ExtractMultiple('1,2,345,,5', 0b10000000000000000));  //'';
