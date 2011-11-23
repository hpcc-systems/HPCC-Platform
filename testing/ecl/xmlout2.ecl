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

//noroxie
phoneRecord := 
            RECORD
string5         areaCode{xpath('areaCode')};
udecimal12      number{xpath('number')};
            END;

contactrecord := 
            RECORD
phoneRecord     phone;
boolean         hasemail{xpath('hasEmail')};
                ifblock(self.hasemail)
string              email;
                end;
            END;

bookRec := 
    RECORD
string      title;
string      author;
    END;

personRecord := 
            RECORD
string20        surname;
string10        forename;
phoneRecord     homePhone;
boolean         hasMobile;
                ifblock(self.hasMobile)
phoneRecord         mobilePhone;
                end;
contactRecord   contact;
dataset(bookRec) books;
set of string    colours;
string2         endmarker := '$$';
            END;

namesTable := dataset([
        {'Halliday','Gavin','09876',123456,true,'07967',838690, 'n/a','n/a',true,'gavin@edata.com',[{'To kill a mocking bird','Lee'},{'Zen and the art of motorcycle maintainence','Pirsig'}], ALL},
        {'Halliday','Abigail','09876',654321,false,'','',false,[{'The cat in the hat','Suess'},{'Wolly the sheep',''}], ['Red','Yellow']}
        ], personRecord);

output(namesTable,,'REGRESS::TEMP::output2.xml',overwrite,xml(heading('','')));

inf := dataset('REGRESS::TEMP::output2.xml', { string text }, csv);
output(inf);

