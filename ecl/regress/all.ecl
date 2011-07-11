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

#option ('globalFold', false);


// Following should be true

all >= all;
all>[1,2];
[1,2]<>all;
all>[1,2];
all>[1,2] AND [1,2]<>all;

[] < ALL;
[] <= ALL;
[] != ALL;
all > [];
all >= [];
all != [];

123 in ALL;
'a' in ALL;

// Following should be false

all > all;
[1,2]>all;
[1,2]=all;
145 not in ALL;

[] > ALL;
[] >= ALL;
[] = all;
all < [];
all <= [];
all = [];
all <= [];

person := dataset('person', { unsigned8 person_id, string10 per_ssn; }, thor);
count(person(per_ssn in ALL));
count(person(per_ssn in []));
