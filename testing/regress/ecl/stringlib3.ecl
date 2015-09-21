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
import Std.Str;


//#IF usage 

#IF(Str.FilterOut('ABCDE', 'BD') = 'ACE')
output('Str.FilterOut(const string src, const string _within)');                            
#ELSE
output('FAILED Str.FilterOut(const string src, const string _within)');                             
#END


#IF(Str.Filter('ABCDE', 'BD') = 'BD')
output('Str.Filter(const string src, const string _within)');                               
#ELSE
output('FAILED Str.Filter(const string src, const string _within)');                                
#END

#IF(Str.SubstituteIncluded('ABCDE', 'BD', 'X') = 'AXCXE')
output('Str.SubstituteIncluded(const string src, const string _within, const string _newchar)');  
#ELSE
output('FAILED Str.SubstituteIncluded(const string src, const string _within, const string _newchar)');  
#END

#IF(Str.SubstituteExcluded('ABCDE', 'BD', 'X') = 'XBXDX')
output('Str.SubstituteExcluded(const string src, const string _within, const string _newchar)');    
#ELSE
output('FAILED Str.SubstituteExcluded(const string src, const string _within, const string _newchar)');     
#END

#IF(lib_stringlib.StringLib.StringRepad('ABCDE   ', 6) = 'ABCDE ')
output('StringLib.StringRepad(const string src, unsigned4 size)');                                      
#ELSE
output('FAILED StringLib.StringRepad(const string src, unsigned4 size)');                                       
#END

#IF(Str.Find('ABCDE', 'BC', 1 ) = 2)
output('Str.Find(const string src, const string tofind, unsigned4 instance )');                 
#ELSE
output('FAILED Str.Find(const string src, const string tofind, unsigned4 instance )');              
#END

ebcdic string a := 'ABCDE';
ebcdic string b := 'BC';
ebcdic string c := 'ABCDEABCDE';

#IF(lib_stringlib.StringLib.EbcdicStringFind(a, b, 1 ) = 2)
output('StringLib.EbcdicStringFind(const ebcdic string src, const ebcdic string tofind , unsigned4 instance )'); 
#ELSE
output('FAILED StringLib.EbcdicStringFind(const ebcdic string src, const ebcdic string tofind , unsigned4 instance )'); 
#END

#IF(Str.Extract('AB,CD,E', 2) = 'CD')
output('Str.Extract(const string src, unsigned4 instance)');                                
#ELSE
output('FAILED Str.Extract(const string src, unsigned4 instance)');                                 
#END

#IF(lib_stringlib.StringLib.GetDateYYYYMMDD() <> '')
output('StringLib.GetDateYYYYMMDD()');                                                                  
#ELSE
output('FAILED StringLib.GetDateYYYYMMDD()');                                                                   
#END

#IF(lib_stringlib.StringLib.GetBuildInfo()<>'')
output('StringLib.GetBuildInfo()');                                                                         
#ELSE
output('FAILED StringLib.GetBuildInfo()');                                                                      
#END

data2 CRLF := x'0a0d';

#IF(lib_stringlib.StringLib.Data2String(CRLF) = '0A0D')
output('StringLib.Data2String(const data src)');                                                        
#ELSE
output('FAILED StringLib.Data2String(const data src)');                                                         
#END

#IF(StringLib.String2Data('4142') = x'4142')
output('StringLib.String2Data(const string src)');                                                      
#ELSE
output('FAILED StringLib.String2Data(const string src)'); 
#END

#IF(Str.ToLowerCase('ABCDE') = 'abcde')
output('Str.ToLowerCase(const string src)');                                                
#ELSE
output('FAILED Str.ToLowerCase(const string src)');                                                 
#END

#IF(Str.ToUpperCase('abcde') = 'ABCDE')
output('Str.ToUpperCase(const string src)');                                                
#ELSE
output('FAILED Str.ToUpperCase(const string src)');                                                 
#END

#IF(Str.ToCapitalCase('abcde') = 'Abcde')
output('Str.ToCapitalCase(const string src)');                                              
#ELSE
output('FAILED Str.ToCapitalCase(const string src)');                                               
#END

#IF(Str.CompareIgnoreCase('ABCDE','abcde') = 0)
output('Str.CompareIgnoreCase(const string src1, string src2)');                            
#ELSE
output('FAILED Str.CompareIgnoreCase(const string src1, string src2)');                             
#END

#IF(Str.Reverse('ABCDE') = 'EDCBA')
output('Str.Reverse(const string src)');                                                    
#ELSE
output('FAILED Str.Reverse(const string src)');                                                     
#END

#IF(Str.FindReplace('ABCDEABCDE', 'BC', 'XY') = 'AXYDEAXYDE')
output('Str.FindReplace(const string src, const string stok, const string rtok)');          
#ELSE
output('FAILED Str.FindReplace(const string src, const string stok, const string rtok)');           
#END

#IF(Str.CleanSpaces('ABCDE    ABCDE   ABCDE') = 'ABCDE ABCDE ABCDE')
output('Str.CleanSpaces(const string src)');                                                
#ELSE
output('FAILED Str.CleanSpaces(const string src)');                                                 
#END

#IF(Str.WildMatch('ABCDE', 'a?c*', true) = true)
output('Str.WildMatch(const string src, const string _pattern, boolean _noCase)');          
#ELSE
output('FAILED Str.WildMatch(const string src, const string _pattern, boolean _noCase)');           
#END

#IF(Str.Contains('ABCDE', 'abc', true) = true)
output('Str.Contains(const string src, const string _pattern, boolean _noCase)');           
#ELSE
output('FAILED Str.Contains(const string src, const string _pattern, boolean _noCase)');            
#END


#IF(Str.ExtractMultiple('1,2,345,,5', 0b0001) = '1')
output('Str.ExtractMultiple(commastring, bitmap)');
#ELSE
output('FAILED Str.ExtractMultiple(commastring, bitmap)');
#END

