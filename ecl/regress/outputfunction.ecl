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

f() := function

    personRecord := RECORD
    unsigned4 personid;
    string1 sex;
    string33 forename;
    string33 surname;
    string4 yob;
    unsigned4 salary;
        END;
    personDataset := DATASET('person',personRecord,thor);


    return parallel(output('begin'),output(personDataset),output('done'));
end;

f();
