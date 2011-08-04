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

ppersonRecord := RECORD
string3 id := '000';
string10 surname := '';
string10 forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
END;

tmptable := dataset([
{'001', 'Hawthorn','Gavin', 13, 10},
{'002', 'Smith','Zack', 13, 10},
{'003', 'Hewit','Nigel', 13, 10},
{'004', 'Gillin','Paul', 13, 10}], ppersonRecord);


s0 := DEDUP(tmptable, forename, ALL);


s1 := SORT(tmptable, surname) : STORED('jaketest_stored11', few);

OUTPUT(s1, , 'out1.d00', OVERWRITE);

