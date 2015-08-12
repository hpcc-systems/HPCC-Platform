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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name, data9 per_cid, unsigned8 __personpos, unsigned8 __masterpos}, thor);
tr001 := COUNT(person);

LayoutProject6_Formatted:= record
String18 Cid;
String9 per_cid;
String3 U1;
String3 U8;
String9 U17;
String4 U21;
String3 U33;
String3 U46;
String3 U53;
String3 U56;
String3 S2;
String3 S8;
String3 S12;
String9 S22;
String4 S23;
String5 S41;
String3 S60;
end;

FileProject6_Formatted := dataset ('Project6_Formatted.d00', layoutProject6_Formatted, flat);

SuperComputerOutput := table (person, {per_cid, tr001, __personpos, __masterpos});

JoinRecord := record
                data9 per_cid;
                boolean ValuesMatchFlag;
                MyValue := tr001;
                CompareValue := fileProject6_formatted.u8;
                integer8 __personpos;
                integer8 __masterpos;
              end;

JoinRecord JoinTransform (SuperComputerOutput l, fileProject6_formatted r) :=
                transform
                   self.ValuesMatchFlag := ((integer2) l.tr001 = (integer2) r.u8);
                   self.MyValue := l.tr001;
                   self.CompareValue := r.u8;
                   self := l;
                 end;

JoinedCompare := join (SuperComputerOutput, fileProject6_formatted,
//                     LEFT.per_cid[1..7] = RIGHT.per_cid[1..7],
                       LEFT.per_cid = (data)RIGHT.per_cid,
                       JoinTransform (LEFT, RIGHT));

output (choosen (JoinedCompare, 100), , 'out.d00');

