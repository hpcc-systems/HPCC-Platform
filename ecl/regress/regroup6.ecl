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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
unsigned        score;
          end;

nameRec := record
string20        name;
unsigned        score;
          end;

resultrec := record(inrec)
dataset(nameRec)    names;
          end;

ds := dataset([1,2,3,4,5,6], inrec);

dsg := group(ds, row);

i1 := dataset([
            {1, 'Gavin', 10}, 
            {2, 'Richard', 5}, 
            {5,'Nigel', 2},
            {0, '', 0}], outrec);
i2 := dataset([
            {1, 'Gavin Halliday', 12}, 
            {2, 'Richard Chapman', 15}, 
            {3, 'Jake Smith', 20},
            {5,'Nigel Hicks', 100},
            {0, '', 0}], outrec);
i3 := dataset([
            {1, 'Halliday', 8}, 
            {2, 'Richard', 8}, 
            {6, 'Pete', 4},
            {6, 'Peter', 8},
            {6, 'Petie', 1},
            {0, '', 0}], outrec);

j1 := join(dsg, i1, left.did = right.did, left outer, many lookup);
j2 := join(dsg, i2, left.did = right.did, left outer, many lookup);
j3 := join(dsg, i3, left.did = right.did, left outer, many lookup);

//perform 3 soap calls in parallel
combined := regroup(j1, j2, j3);

resultRec t(inrec l, dataset(recordof(combined)) r) := transform
    self.names := project(r, transform(nameRec, self := left));
    self := l;
    end;

results := combine(dsg, combined, group, t(LEFT, ROWS(RIGHT)(score != 0)));
output(results);

//A variation using rows in a child query.
resultRec t2(inrec l, dataset(recordof(combined)) r) := transform
    self.names := project(sort(r, -score), transform(nameRec, self := left));
    self := l;
    end;

results2 := combine(dsg, combined, group, t2(LEFT, ROWS(RIGHT)(score != 0)));
output(results2);

