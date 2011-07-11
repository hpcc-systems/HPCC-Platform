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

testRecord := RECORD
string10            forename;
string10            surname;
string6             strsalary;
string2             nl;
                END;

test2Record := RECORD
string10            newforename;
string10            newsurname;
string6             newstrsalary;
string2             newnl;
                END;

testDataset := DATASET('inagg.d00', testRecord, FLAT);


x1 := table(testDataset,{surname,count(group)},testDataset.surname);
x2 := sort(x1, surname);

x3 := sort(testDataset, surname);
x4 := table(x3,{surname,count(group)},surname);

test2Record t(testRecord l) := TRANSFORM
        SELF.newforename := l.forename;
        SELF.newsurname := l.surname;
        SELF.newstrsalary := l.strsalary;
        SELF.newnl := l.nl;
    END;

x5 := project(x3, t(LEFT));
x6 := table(x5,{newsurname,count(group)},newsurname);

output(x2,,'out3.d00');
output(x4,,'out4.d00');
output(x6,,'out5.d00');
