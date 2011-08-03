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

LayoutXYZSection := record
 integer4 record_id;
 integer2 XYZ_len;
 integer1 XYZ_flg1;
 integer1 XYZ_flg2;
 integer1 XYZ_flg3;
 integer1 XYZ_flg4;
 integer1 XYZ_sf_1;
 integer1 XYZ_sf_2;
 integer1 ecmd_len;
 integer1 ecmd_flags;
 ebcdic string25 XYZ_cakey;
 ebcdic string25 XYZ_fakey;
 ebcdic string25 XYZ_f2key;
 string1 end_of_record;
end;
XYZFile := dataset ('ecl_test::smth_XYZ', LayoutXYZSection, flat);

/*Change EBCDIC to ASCII*/

LayoutXYZSectionNew := record
 string14 XYZ_newkey;
 end;


layoutXYZSectionNew XYZTrans(layoutXYZSection L) := transform

self.XYZ_newkey := 'boo';
self := L;


end;



DOut := project(XYZFile,XYZTrans(left));

layoutXYZSectionNew XYZTrans2(layoutXYZSectionNew L) := transform

self.XYZ_newkey :='Hello';

end;



output(project(Dout,XYZtrans2(left)))
