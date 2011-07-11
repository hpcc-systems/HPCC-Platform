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

import lib_logging;

ppersonRecord := RECORD
string10    surname ;
string10    forename;
string2     nl;
  END;


ppersonRecordEx := RECORD
string10    surname ;
string10    forename;
integer1    age;
unsigned1   sex;        // 1 == male, 0 == female
string2     nl;
    END;


warnValue(string text, integer value) := function
    logging.addWorkunitWarning(text);
    return value;
end;

ppersonRecordEx projectFunction(ppersonRecord incoming) := Transform
    SELF.age := warnValue('Help',33);
    SELF.sex := if(incoming.surname='',warnValue('Invalid: '+incoming.forename,1),2);

    SELF := incoming;
END;


pperson := DATASET([{'Halliday','Gavin',''},{'','James',''}], ppersonRecord);

ppersonEx := project(pperson, projectFunction(left));

output(ppersonEx);



