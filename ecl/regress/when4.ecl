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

storedTrue := true : STORED('storedTrue');
msgRec := { unsigned4 code, string text; };

boolean isOdd(unsigned4 x) := (x & 1)=1;

ds1 := NOFOLD(DATASET([1,2,3,4,5,6,7], { INTEGER id }));
successMsg := nofold(dataset([{0,'Success'}], msgRec));
failureMsg(unsigned4 code) := nofold(dataset([{code,'Failure'}], msgRec));

when1 := WHEN(ds1, output(successMsg,NAMED('Messages'),EXTEND), success);

output(when1(isOdd(id)));

oCond := IF(storedTrue, output(failureMsg(10),NAMED('Messages'),EXTEND), output('Incorrect'));

when2 := WHEN(ds1, oCond, success);

output(when2(not isOdd(id)));
