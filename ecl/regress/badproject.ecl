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


export pperson := DATASET('in.d00', ppersonRecord, FLAT);

ppersonRecord := RECORD
string10    surname ;
string10    forename;
string2     nl;
  END;

ppersonRecordEx := RECORD
string10    surname ;
string10    forename;
integer1    age;
boolean     sex;        // 1 == male, 0 == female
string2     nl;
    END;


ppersonRecordEx projectFunction(ppersonRecord incoming) := Transform
    SELF.age := 33;
    SELF.sex := 1;
    SELF := incoming;
END;



ppersonEx := project(pperson, projectFunction(left));

output(ppersonEx, , 'out.d00');



