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

#option ('optimizeGraph', false);
#option ('globalFold', false);
// optfilter
// filter should be moved before the project.  Also a good example for
// read/filter/project optimization

testRecord := RECORD
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
integer2  age;
    END;

test := DATASET('test',testRecord,FLAT);

taR := RECORD
        testRecord;
    END;

taR ta(test le) := TRANSFORM
        SELF := le;
    END;

a := project(test,ta(LEFT));

b:= a(per_surname <> 'Hawthorn' and a.per_forename <> 'Gavin');

output(b,,'out.d00');

tcR := RECORD
        test;
    END;

tcR tc(test le) := TRANSFORM
        SELF := le;
    END;

c := project(test,tc(LEFT));

tdR := RECORD
        c;
    END;

tdR td(c le) := TRANSFORM
        SELF := le;
    END;

d := project(c,td(LEFT));

e := d(d.holepos > 10 and per_surname <> '');

output(e,,'oute.d00');
