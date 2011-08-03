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
