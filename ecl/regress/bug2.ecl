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
