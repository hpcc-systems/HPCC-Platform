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

ppersonRecord := RECORD
string10    surname ;
string10    forename;
string2     nl;
  END;


ppersonRecordEx := RECORD
string10    surname ;
string10    forename;
integer1    age;
string1     sex;
string2     nl;
unsigned4   seq;
    END;



ppersonRecordEx projectFunction(ppersonRecord incoming, unsigned4 c, string1 sex) := Transform
    SELF.age := 33;
    SELF.sex := sex;
    SELF.seq := c;
    SELF := incoming;
END;


pperson := DATASET('in.d00', ppersonRecord, FLAT);

ppersonEx := project(pperson, projectFunction(left, COUNTER, 'M'));

f := ppersonEx(age != 10);
output(f, , 'out.d00');

ppersonEx2 := project(pperson, projectFunction(left, COUNTER, 'F'));

f2 := ppersonEx2(seq != 10);
output(f2, , 'out.d00');

ppersonEx3 := project(pperson, projectFunction(left, COUNTER, 'N'));
f3 := ppersonEx3(age = 10);
output(f3, , 'out.d00');

