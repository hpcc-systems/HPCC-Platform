/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//noroxie
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

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
BITFIELD63      b1;
BITFIELD1       b2;
BITFIELD3_8     b3;
BITFIELD12_8    b4;
BITFIELD17_8    b5;
string2         endmarker := '$$';
            END;

namesTable := dataset([
        {'Halliday','Gavin','09876',123456,true,'07967',838690, 'n/a','n/a',true,'gavin@edata.com',[{'To kill a mocking bird','Lee'},{'Zen and the art of motorcycle maintainence','Pirsig'}], ALL, 1234567,0,7,6,5},
        {'Halliday','Abigail','09876',654321,false,'','',false,[{'The cat in the hat','Suess'},{'Wolly the sheep',''}], ['Red','Yellow'], 0x7fffffffffffffff,1,7,4095,0x1FFFF}
        ], personRecord);

output(namesTable,,prefix + 'TEMP_output2.xml',overwrite,xml(heading('','')));
output(count(nofold(namesTable)(b1 > 0x80000000)));
inf := dataset(prefix + 'TEMP_output2.xml', { string text }, csv);
output(inf);

