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
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);

i1 := index(d, { d } ,'\\seisint\\person.name_first.key1');
nameIndexRecord := recordof(i1);

errorRecord := RECORD
unsigned4           code;
string50            msg;
               END;

fullRecord := RECORD(namesRecord)
errorRecord     err;
            END;

fullRecord t(nameIndexRecord l) := transform
    SELF := l;
    SELF := [];
END;

fullRecord createError(unsigned4 code, string50 msg) := transform
    SELF.err.code := code;
    SELF.err.msg := msg;
    SELF := [];
END;

//Illegal return type
res3a := fetch(d, i1(surname='Jones'), right.filepos);
res3b := limit(res3a, 99, ONFAIL(createError(99, 'Too many matching names 3')));
output(res3b);
