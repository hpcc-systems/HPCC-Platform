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
usports := record
string ports;
end;

ds1 := dataset('in::rawusports', usports, csv);

lp := length(ds1.ports);
newstruct := record
integer4 portnumber := (integer4)( ds1.ports[1..4]);
string2 state := if (ds1.ports[lp-2] = ',' or ds1.ports[lp-2] = ' ', ds1.ports[lp-1.. lp], '');
string portname :=if (ds1.ports[lp-2] = ',', ds1.ports[6.. lp-3],if (ds1.ports[lp-2] = ' ', ds1.ports[6..lp-4],ds1.ports[6..lp]));
end;

newports := table(ds1, newstruct);

exportports := record
string portname;
string restofit;
end;

ep := dataset('in::rawexportports', exportports, csv);

integer last4(string x) := (integer)(x[(length(x)-4)..length(x)]);

newstruct mapexport(exportports l) := transform
// Map the two formats together
self.portname := l.portname;
self.state := l.restofit[1..2];
self.portnumber := last4(l.restofit);
end;

exset := project(ep, mapexport(left));

// Not available in the downloaded data - still need 2401 and 5302 as well.

dsaddthem := dataset([{2098,'TN','Memphis FTZ'}, {2308, 'TX', 'San Antonio'},
    {3411, 'ND', 'Fargo'}, {4113, 'IN', 'Evansville'}], newstruct);

outset := exset + newports+dsaddthem;
outsort := sort(outset, portnumber);
outdedup := dedup(outsort, portnumber);
output(outdedup,,'customs::usports', overwrite);

