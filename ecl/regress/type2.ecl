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

parentRecord :=
                RECORD
unsigned8           id;
string20            name1_last;
string2             st;
string5             z5;
unsigned2           numPeople;
string10            prim_name;
string5             prim_range;
string5             reason;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


string5 bword(string le,string ri,string5 wo) := if ( le = '' OR ri
= '', 'BLANK',wo);


parentRecord t(parentRecord le, parentRecord ri) := TRANSFORM

self.reason := MAP(
  le.name1_last <> ri.name1_last => bword(le.name1_last,ri.name1_last,'NAME'),
//  le.st <> ri.st => bword(le.st,ri.st,'STATE'),
//  le.z5 <> ri.z5 => bword(le.z5,ri.z5,'ZIP5 '),
//  le.prim_name <> ri.prim_name => bword(le.prim_name,ri.prim_name,'STRET'),
//  le.prim_range <> ri.prim_range => bword(le.prim_range,ri.prim_range,'RANGE'),
  'ERROR');
  self := le;
  end;

z := join(parentDataset, parentDataset, LEFT.numPeople = RIGHT.numPeople, t(LEFT, RIGHT));

output(z);
