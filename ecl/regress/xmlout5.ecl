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

#option ('targetClusterType', 'hthor');
filePrefix := 'GAVIN';

//UseStandardFiles
//noroxie           - shame but it outputs to files etc.
TOXMLPREFIX := '~REGRESS::' + filePrefix + '::RESULT::';

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
        {U'Hawthorn','Abigäil','09876',123987,false,'','',false,[{U'The cat in the hat','Suess'},{U'Wolly the sheep',''}], ['Red','Yellow']}
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

p := TABLE(namesTable,{utf8 text{maxlength(1024)} := U'<row>'+toxml(row(namesTable))+U'</row>'});

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
output(dataset(TOXMLPREFIX+'toxml1.xml', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml3.xml', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml4.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml5.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml6.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
output(dataset(TOXMLPREFIX+'toxml7.utf8', { unicode line{maxlength(4096)} }, csv(unicode)));
