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

integer4 basemyint := 99;
integer4 myInt := if(exists(dataset(workunit('isrecovering'), {boolean flag})), 1, basemyint);

myrec := RECORD
            string30 addr;
END;

myDS := dataset([{'359 Very Rocky River Dr'}],myrec);

outrec := if(myint<2,myDS,FAIL(myrec,99,'ouch')) : RECOVERY(output(dataset([true],{boolean flag}),,named('isRecovering')));

output(count(outrec),NAMED('Count'));

output(outrec,NAMED('Outrec'));


