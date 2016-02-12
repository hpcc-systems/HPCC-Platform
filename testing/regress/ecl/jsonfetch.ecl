/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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
        {'Shakespeare','William', 1, 1, '09876',123456,true,'07967',838690, 'n/a','n/a',true,'william@edata.com',[{'To kill a mocking bird','Lee'},{'Zen and the art of motorcycle maintainence','Pirsig'}], ALL},
        {'Mitchell','Margaret', 1, 1, '09876',123456,true,'07967',838690, 'n/a','n/a',true,'maggy@edata.com',[{'Harry Potter and the Deathly Hallows','Rowling'},{'Where the Sidewalk Ends','Silverstein'}], ['Violet','Orange']},
        {'Mitchell','David', 1, 1, '09876',123456,true,'07967',838690, 'n/a','n/a',true,'dm@edata.com',[{'Love in the Time of Cholera','Marquez'},{'Where the Wild Things Are','Sendak'}], ALL},
        {'Dickens','Charles', 1, 2, '09876',654321,false,'','',false,[{'The cat in the hat','Suess'},{'Wolly the sheep',''}], ['Red','Yellow']},
        {'Rowling','J.K.', 1, 2, '09876',654321,false,'','',false,[{'Animal Farm',''},{'Slaughterhouse-five','Vonnegut'}], ['Blue','Green']}
        ], personRecord);

output(namesTable,,'REGRESS::TEMP::jsonfetch.json',overwrite, json);
dsJsonFetch := dataset(DYNAMIC('REGRESS::TEMP::jsonfetch.json'), personRecord, json('Row'));

dsJsonFetchWithPos := dataset(DYNAMIC('REGRESS::TEMP::jsonfetch.json'), {personRecord, UNSIGNED8 RecPtr{virtual(fileposition)}}, json('Row'));
BUILD(dsJsonFetchWithPos, {surname, RecPtr}, 'REGRESS::TEMP::jsonfetch.json.index', OVERWRITE);

jsonFetchIndex := INDEX(dsJsonFetchWithPos, {surname, RecPtr}, DYNAMIC('REGRESS::TEMP::jsonfetch.json.index'));

fetcheddata := LIMIT(FETCH(dsJsonFetchWithPos, jsonFetchIndex(surname = 'Mitchell'), RIGHT.RecPtr), 10);
fetchednopos := project(fetcheddata, personRecord); //don't output positions
output(fetchednopos, named('fetched'));
