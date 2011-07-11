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

#option ('targetClusterType', 'roxie');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);
s := sort(namesTable, forename);


ds1 := table(dataset([{0}],{integer i}), 
{
'--',
if(count(s) > 0, s[1].age, 0),
if(count(s) > 0, s[1].forename, ''),
if(count(s) >= 1, s[1].forename, ''),
if(count(s) > 0, s[2].age, 0),
if(count(s) > 1, s[2].forename, ''),
'--'
});

output(ds1);


ds2 := table(dataset([{5}],{integer i}), 
{
'--',
if(count(s) > 1, s[1].forename, ''),
if(count(s) > 3, s[2].age, 0),          // can't optimize to s[2].age, since differ for count=2/3
if(count(s) > 3, s[2].forename, ''),
'--'
});

output(ds2);


