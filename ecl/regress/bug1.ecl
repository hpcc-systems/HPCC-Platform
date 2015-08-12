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
