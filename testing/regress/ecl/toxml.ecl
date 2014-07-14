/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//UseStandardFiles
TOXMLPREFIX := '~REGRESS::' + __PLATFORM__ + '::RESULT::';

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

output(U'<row>'+(utf8)U'εν αρχη ην ο λογος'+U'</row>');
output(U'<row>'+U8'εν αρχη ην ο λογος'+U'</row>');
output(U8'<row>'+U8'εν αρχη ην ο λογος'+U8'</row>');
output((utf8)(U'<row>'+U'εν αρχη ην ο λογος'+U'</row>'));

output(namesTable,,TOXMLPREFIX+'toxml1.xml',expire(1),overwrite,xml(heading('<MyDataset>\n','</MyDataset>')));
output(namesTable,,TOXMLPREFIX+'toxml2.flat',expire(1),overwrite);

//Read from xml and then write out again - should be possible to compare the results easily
inf := dataset(TOXMLPREFIX+'toxml1.xml', personRecord, xml('/MyDataset/Row'));
output(inf,,TOXMLPREFIX+'toxml3.xml',expire(1),overwrite,xml);

output(inf,{string xml{maxlength(1024)} := (string)toxml(row(inf))},TOXMLPREFIX+'toxml4.utf8',expire(1),overwrite,csv(unicode));
output(inf,{utf8 xml{maxlength(1024)} := ((utf8)U'<row>')+toxml(row(inf))+((utf8)U'</row>')},TOXMLPREFIX+'toxml5.utf8',expire(1),overwrite,csv(unicode));
output(inf,{unicode xml{maxlength(1024)} := toxml(row(inf))},TOXMLPREFIX+'toxml6.utf8',expire(1),overwrite,csv(unicode));

p := TABLE(namesTable,{unicode text{maxlength(1024)} := U'<row>'+toxml(row(namesTable))+U'</row>'});

personRecord createRowFromXml() := transform
    self.surname := (unicode)xmltext('surname');
    self.forename := xmlunicode('forename');
    self := [];
END;

z := parse(p, p.text, createRowFromXml(), xml('/row'));
output(z);

infraw := dataset(TOXMLPREFIX+'toxml2.flat', personRecord, thor);
output(infraw,{string xml{maxlength(1024)} := (string)toxml(row(infraw))},TOXMLPREFIX+'toxml7.utf8',expire(1),overwrite,csv(unicode));

//Now read each of the xml files and output to the work unit as csv so we can check the content!
output(dataset(TOXMLPREFIX+'toxml1.xml', { unicode line{maxlength(4096)} }, csv(unicode))(line not in ['<MyDataset>','</MyDataset>']));
output(dataset(TOXMLPREFIX+'toxml3.xml', { unicode line{maxlength(4096)} }, csv(unicode))(line not in ['<Dataset>','</Dataset>']));
output(dataset(TOXMLPREFIX+'toxml4.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml5.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml6.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml7.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
