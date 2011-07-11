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

phoneRecord := 
            RECORD
string5         areaCode{xpath('@areaCode')};
string12        number{xpath('@number')};
            END;

nullPhones := dataset('ph', phoneRecord, thor);

contactrecord := 
            RECORD
phoneRecord     phone;
boolean         hasemail;
                ifblock(self.hasemail)
string              email;
                end;
            END;

personRecord := 
            RECORD
string20        surname;
string20        forename;
set of string   middle{xpath('MiddleName/Instance')} := [];
//dataset(phoneRecord) phones{xpath('/Phone')} := dataset([{ '', ''}], phoneRecord)(false);
dataset(phoneRecord) phones{xpath('/Phone')} := _EMPTY_(phoneRecord); //nullPhones;
            END;

namesTable := dataset([
        {'Halliday','Gavin'},
        {'Halliday','Abigail'}
        ], personRecord);

personRecord t(personrecord l) := transform
        SELF.middle := IF(l.forename = 'Gavin', ['Charles'], ['Anabelle','Spandex']);
        SELF := l;
        END;

namesTable2 := project(namesTable, t(left));
output(namesTable2,,'people.xml',overwrite,xml);
