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

import dt;

namesRecord :=
            RECORD
dt.pstring      surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

output(namesTable, {length(surname),length(forename),trim(surname), surname IN ['a','b','c','d','e','f','0','1','2','3','4','5','6','7','8','9']});

unamesRecord :=
            RECORD
dt.ustring(u'\r')   surname;
            END;

unamesTable := dataset('xu',unamesRecord,FLAT);

output(unamesTable, {length(surname),trim(surname), surname IN [u'a',u'b',u'c',u'd',u'e',u'f',u'0',u'1',u'2',u'3',u'4',u'5',u'6',u'7',u'8',u'9']});
