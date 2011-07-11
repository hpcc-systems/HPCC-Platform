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
            end;

finalRec := record(inrec)
dataset(nameRec)    names;
string20            secondName;
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

j1 := join(dsg, i1, left.did = right.did, transform(outrec, self := left; self := right), left outer, many lookup);
j2 := join(dsg, i2, left.did = right.did, transform(outrec, self := left; self := right), left outer, many lookup);
j3 := join(dsg, i3, left.did = right.did, transform(outrec, self := left; self := right), left outer, many lookup);

//perform 3 soap calls in parallel
combined := regroup(j1, j2, j3);

finalRec doRollup(outRec l, dataset(outRec) allRows) := transform
    self.did  := l.did;
    self.names := project(allRows(score != 0), transform(nameRec, self := left));
    self.secondName := allRows(score != 0)[2].name;
    end;

results := rollup(combined, group, doRollup(left, rows(left)));

output(results);

//Same as above, but calls a function, so requires rows to be converted to inline form

rtl := service
dataset(recordof(combined)) dataset2DatasetX(const dataset src) :                   eclrtl,include,pure,library='eclrtl',entrypoint='rtlStrToDataX';
     end;


finalRec doRollup2(outRec l, dataset(outRec) allRows) := transform
    self.did  := l.did;
    self.names := project(rtl.dataset2DatasetX(allRows)(score != 0), transform(nameRec, self := left));
    self.secondName := '';
    end;

results2 := rollup(combined, group, doRollup2(left, rows(left)));

output(results2);

//Check result isn't grouped - result should be a single value
output(results2, { cnt := count(group) });
