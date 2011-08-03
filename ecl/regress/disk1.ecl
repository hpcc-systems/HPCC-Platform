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

personRecord := RECORD
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

newPersonRecord := RECORD
unsigned4 person_id;
string20  surname;
    END;

personIdRecord := RECORD
unsigned4 person_id;
    END;

personDataset := DATASET('person',personRecord,FLAT);

o1 := personDataset(surname <> 'Hawthorn');
output(o1,,'out1.d00');

o2 := personDataset(surname <> 'Hawthorn',forename <>'Gavin');
output(o2,,'out2.d00');

newPersonRecord t(personRecord l) := TRANSFORM SELF := l; END;
personidRecord t2(newPersonRecord l) := TRANSFORM SELF := l; END;

o3 := project(personDataset, t(LEFT));
output(o3,,'out3.d00');

o4a := personDataset(surname <> 'Page');
o4b := project(o4a, t(LEFT));
output(o4b,,'out4.d00');

o5a := project(personDataset, t(LEFT));
o5b := o5a(surname <> 'Hewit');
output(o5b,,'out5.d00');

o6a := personDataset(forename <> 'Richard');
o6b := project(o6a, t(LEFT));
o6c := o6b(surname <> 'Drimbad');
output(o6c,,'out6.d00');

o7a := project(personDataset, t(LEFT));
o7b := project(o7a, t2(LEFT));
output(o7b,,'out7.d00');

o8a := personDataset(forename <> 'Richard');
output(o8a,{forename,surname},'out8.d00');

o9a := personDataset(forename <> 'Richard');
o9b := table(o9a,{forename,surname});
output(o9b,,'out9.d00');

o10a := personDataset(forename <> 'Richard');
o10b := table(o10a,{count(group),surname},surname);
output(o10b,,'out10.d00');

