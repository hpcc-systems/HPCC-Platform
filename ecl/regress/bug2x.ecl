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

LayoutName := record
  string2 f_sfx;
  string2 f_sx;
  integer4 aa_record_id;
end;

NameFile := dataset ('ecl_test::smth_aa', LayoutName, flat);

layoutNameOut := record
  integer4 record_id;
  string2 f_sfx;
end;

layoutNameOut NameTrans(layoutName L) := transform
  self.f_sfx := map (L.f_sfx in ['0 ', '1'] => L.f_sx, '  ');
  self.record_id := L.aa_record_id;
  self := L;
end;

DOut := project(NameFile, NameTrans(left));

output(dOut,, 'NameFile_ascii2ebc.d00');
