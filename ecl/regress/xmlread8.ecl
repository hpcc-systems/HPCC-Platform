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

nameRecord :=
                RECORD
string              idx{xpath('@index')};
string              name{xpath('')};
                END;

instrumentRecord := RECORD,maxLength(10000)
udecimal10              id{xpath('@id')};
integer                 book{xpath('BOOK')};
dataset(nameRecord)     names{xpath('NAME')};
set of string           legals{xpath('LEGAL')};
dataset(nameRecord)     names2;
set of string           legals2;
dataset(nameRecord)     names3{xpath('NAMES/NAME')};
set of string           legals3{xpath('LEGALS/LEGAL')};
set of string           legals4{xpath('LEGALS/LEGAL[@type=\'legal\']/@xyz')};
set of string           legals5{xpath('/LEGAL/@xyz')};
                END;


test := dataset('~file::127.0.0.1::temp::20040621_vclerk.xml', instrumentRecord, XML('/VolusiaClerk/OfficialRecords/Instrument'));
output(choosen(test,100),,'out2.d00',XML,overwrite);
