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
unicode     title;
string      author;
    END;

personRecord :=
            RECORD
unicode20       surname;
unicode10       forename;
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
        {U'Hälliday','Gavin','09876',123987,true,'07967',123987, 'n/a','n/a',true,'gavin@edata.com',[{U'εν αρχη ην ο λογος', 'john'}, {U'To kill a mocking bird','Lee'},{U'Zen and the art of motorcycle maintainence','Pirsig'}], ALL},
        {U'Halliday','Abigäil','09876',123987,false,'','',false,[{U'The cat in the hat','Suess'},{U'Wolly the sheep',''}], ['Red','Yellow']}
        ], personRecord);

output(namesTable);

output(namesTable, {unicode json{maxlength(1024)} := U'{' + tojson(row(namesTable)) + U'}'});

output(namesTable, {unicode xml{maxlength(1024)} := U'<Row>' + toxml(row(namesTable)) + U'</Row>'});

