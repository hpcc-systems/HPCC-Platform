/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

    This program is free software: you can redistribute it and/or modify
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
    u8'{"surname": "Hälliday", "forename": "Gavin", "homephone": {"@areaCode": "09876", "@number": "123987"}, "hasmobile": true, "mobilephone": {"@areaCode": "07967", "@number": "123987"}, ' +
        u8'"contact": {"@hasEmail": true, "phone": {"@areaCode": "n/a", "@number": "0"}, "email": "gavin@edata.com"}, ' +
        u8'"books": { "Row": [{"title": "εν αρχη ην ο λογος", "author": "john"}, {"title": "To kill a mocking bird", "author": "Lee"}, {"title": "Zen and the art of motorcycle maintainence", "author": "Pirsig"}]}, "colours": {"All": true}, "endmarker": "$$"}',

    u8'{"surname": "Halliday", "forename": "Abigäil", "homephone": {"@areaCode": "09876", "@number": "123987"}, "hasmobile": false, "contact": {"@hasEmail": false, "phone": {"@areaCode": "", ' +
    u8'"@number": "0"}}, "books": {"Row": [{"title": "The cat in the hat", "author": "Suess"}, {"title": "Wolly the sheep", "author": ""}]}, "colours": {"Item": ["Red", "Yellow"]}, "endmarker": "$$"}'
    ], { utf8 text; });

output(namesTable, { FROMJSON(personRecord, text, TRIM); });
output(1);
