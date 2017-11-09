/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

import $.setup;
prefix := setup.Files(false, false).FilePrefix;

phoneRecord :=
            RECORD
string5         areaCode{xpath('@areaCode')};
udecimal12      number{xpath('@number')};
            END;

contactrecord :=
            RECORD
phoneRecord     phone;
boolean         hasemail{xpath('@hasEmail')};
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
integer4        category;
integer8        uid;
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
        {'Halliday','Gavin', 1, 1, '09876',123456,true,'07967',838690, 'n/a','n/a',true,'gavin@edata.com',[{'To kill a mocking bird','Lee'},{'Zen and the art of motorcycle maintainence','Pirsig'}], ALL},
        {'Halliday','Abigail', 1, 2, '09876',654321,false,'','',false,[{'The cat in the hat','Suess'},{'Wolly the sheep',''}], ['Red','Yellow']}
        ], personRecord);

output(namesTable,,prefix + 'output_object_namedArray.json',overwrite, json);
readObjectNamedArray := dataset(DYNAMIC(prefix + 'output_object_namedArray.json'), personRecord, json('Row'));
output(readObjectNamedArray, named('ObjectNamedArray'));

output(namesTable,,prefix + 'output_array.json',overwrite, json('', heading('[', ']')));
readArrayOfRows := dataset(DYNAMIC(prefix + 'output_array.json'), personRecord, json(''));
output(readArrayOfRows, named('ArrayOfRows'));

output(namesTable,,prefix + 'output_noroot.json',overwrite, json('', heading('','')));
readNoRootRows := dataset(DYNAMIC(prefix + 'output_noroot.json'), personRecord, json('', NOROOT));
output(readNoRootRows, named('noRootRows'));

