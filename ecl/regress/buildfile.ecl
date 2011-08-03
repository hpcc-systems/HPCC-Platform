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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

teenagers := dataset(namesTable(age between 13 and 19), 'teenagers', thor);

count(teenagers(surname = 'Hawthorn'));

oldies :=  dataset(namesTable(age >= 65), 'oldies', thor);

build(oldies,overwrite);
build(oldies, 'snowbirds',named('BuildSnowbirds'));

count(oldies(surname = 'Hawthorn'));

babies :=  dataset(namesTable(age < 2), 'babies', thor);

build(babies, persist,backup, update, expire(99));

count(babies(surname = 'Hawthorn'));
