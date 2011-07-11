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

twenty    := 20 : stored('twenty');
twentyone := 21 : stored('twentyone');
twentytwo := 22 : stored('twentytwo');


d1 := dataset([{'Jones','Jim',twenty}], namesRecord);
d2 := dataset([{'Jones','John',twentyone}], namesRecord);
d3 := dataset([{'Jones','James',twentytwo}], namesRecord);
d4 := dataset([{'Jones','Jimmy',twenty}], namesRecord);
d5 := dataset([{'Jones','Jonnie',twentyone}], namesRecord);

output(nonempty(d1(age != 40), d2(age != 40)));
output(nonempty(d1(age != 20), d2(age != 40)));
output(nonempty(d1(age != 40), d2(age != 21)));
output(nonempty(d1(age != 20), d2(age != 21)));
output(nonempty(d1, d2, d3, d4));
output(nonempty(d1(age = 0), d2(age = 0), d3(age = 0), d4));
output(nonempty(d1(age = 0), d2(age = 0), d3, d4));

