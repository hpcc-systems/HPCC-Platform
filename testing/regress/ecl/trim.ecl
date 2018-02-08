/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

// Test variations: left, right, both, all
// Test variations: normal, whitespace
// Test variations: utf8, string, varstring, unicode, varunicode
// Test variations: const folded, not const folded
// Test variations: spaces present, spaces absent, whitespace present, empty, all whitespace

declare_tests(testtype) := MACRO
#uniquename(rawinput)
#uniquename(input)
#uniquename(output)
#uniquename(trans)
%rawinput% := DATASET([{''},{' '},{' \t a b\tc \t '},{'abc'}], { testtype s; });
%input% := NOFOLD(%rawinput%);

%output% := RECORD
 testtype s1; 
 testtype s2; 
 testtype s3; 
 testtype s4; 
 testtype s5; 
 testtype s6; 
 testtype s7; 
 testtype s8; 
END;

%output% %trans%(%input% L) := TRANSFORM
  SELF.s1 := TRIM(L.s, LEFT);
  SELF.s2 := TRIM(L.s, RIGHT);
  SELF.s3 := TRIM(L.s, LEFT, RIGHT);
  SELF.s4 := TRIM(L.s, ALL);
  SELF.s5 := TRIM(L.s, LEFT, WHITESPACE);
  SELF.s6 := TRIM(L.s, RIGHT, WHITESPACE);
  SELF.s7 := TRIM(L.s, LEFT, RIGHT, WHITESPACE);
  SELF.s8 := TRIM(L.s, ALL, WHITESPACE);
END;

OUTPUT(project(%rawinput%, %trans%(LEFT)));
OUTPUT(project(%input%, %trans%(LEFT)));

ENDMACRO;

declare_tests(utf8);
declare_tests(string);
declare_tests(varstring);
declare_tests(unicode);
declare_tests(varunicode);