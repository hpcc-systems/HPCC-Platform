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

#option ('foldAssign', false);
#option ('globalFold', false);
LayoutName := record
unsigned integer1 section_header;
unsigned integer4 aa_record_id;
ebcdic string25 f_ln;
ebcdic string15 f_fn;
ebcdic string15 f_mn;
ebcdic string2 f_sfx;
ebcdic string15 f_spfn;
ebcdic string15 f_spmn;
ebcdic string1 to_end;
end;
NameFile := dataset ('ecl_test::smth_aa', LayoutName, flat);

layoutNameOut := record
//integer1 section_header;
integer4 record_id;
string25 f_ln;
string15 f_fn;
string15 f_mn;
string2 f_sfx;
string15 f_spfn;
string15 f_spmn;
//string1 to_end;
end;
layoutNameOut NameTrans(layoutName L) := transform
self.f_sfx := map (L.f_sfx in ['0 ', '1 ', '2 ', '3 ', '4 ', '5 ', '6 ',
                          '7 ', '8 ', 'JR', 'SR', 'I ', 'II', 'J ',
                          'S '] => L.f_sfx, '  ');
self.record_id := L.aa_record_id;



self := L;
end;
DOut := project(NameFile,NameTrans(left));

output(dOut,, 'NameFile_ascii2ebc.d00');
