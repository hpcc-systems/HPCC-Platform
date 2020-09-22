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

//UseStandardFiles
//version forceLayoutTranslation=0
//version forceLayoutTranslation=1
//version forceLayoutTranslation=2

import $.setup;
import ^ as root;

forceLayoutTranslation := #IFDEFINED(root.forceLayoutTranslation, 0);
prefix := setup.Files(false, false, false, IF(forceLayoutTranslation > 0, '_' + (STRING) forceLayoutTranslation, '')).QueryFilePrefix;

LOADXML('');
#DECLARE(translationMode);
#IF (forceLayoutTranslation = 1)
#SET(translationmode, 'alwaysECL');
#ELSIF (forceLayoutTranslation = 2)
#SET(translationmode, 'alwaysDisk');
#ELSE
#SET(translationmode, 'on');
#END

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
        ], personRecord, HINT(layoutTranslation, %translationmode%));

output(U'<row>'+(utf8)U'εν αρχη ην ο λογος'+U'</row>');
output(U'<row>'+U8'εν αρχη ην ο λογος'+U'</row>');
output(U8'<row>'+U8'εν αρχη ην ο λογος'+U8'</row>');
output((utf8)(U'<row>'+U'εν αρχη ην ο λογος'+U'</row>'));

setup1 := output(namesTable,,prefix+'toxml1.xml',expire(1),overwrite,xml(heading('<MyDataset>\n','</MyDataset>')));
setup2 := output(namesTable,,prefix+'toxml2.flat',expire(1),overwrite);

//Read from xml and then write out again - should be possible to compare the results easily
inf := dataset(prefix+'toxml1.xml', personRecord, xml('/MyDataset/Row'), HINT(layoutTranslation, %translationmode%));
setup3 := output(inf,,prefix+'toxml3.xml',expire(1),overwrite,xml);

setup4 := output(inf,{string xml{maxlength(1024)} := (string)toxml(row(inf))},prefix+'toxml4.utf8',expire(1),overwrite,csv(unicode));
setup5 := output(inf,{utf8 xml{maxlength(1024)} := ((utf8)U'<row>')+toxml(row(inf))+((utf8)U'</row>')},prefix+'toxml5.utf8',expire(1),overwrite,csv(unicode));
setup6 := output(inf,{unicode xml{maxlength(1024)} := toxml(row(inf))},prefix+'toxml6.utf8',expire(1),overwrite,csv(unicode));

p := TABLE(namesTable,{unicode text{maxlength(1024)} := U'<row>'+toxml(row(namesTable))+U'</row>'});

personRecord createRowFromXml() := transform
    self.surname := (unicode)xmltext('surname');
    self.forename := xmlunicode('forename');
    self := [];
END;

z := parse(p, p.text, createRowFromXml(), xml('/row'));
output(z);

infraw := dataset(prefix+'toxml2.flat', personRecord, thor, HINT(layoutTranslation, %translationmode%));
setup7 := output(infraw,{string xml{maxlength(1024)} := (string)toxml(row(infraw))},prefix+'toxml7.utf8',expire(1),overwrite,csv(unicode));

//Now read each of the xml files and output to the work unit as csv so we can check the content!
sequential(
setup1;
setup2;
setup3;
setup4;
setup5;
setup6;
setup7;
output(dataset(prefix+'toxml1.xml', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%))(line not in ['<MyDataset>','</MyDataset>']));
output(dataset(prefix+'toxml3.xml', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%))(line not in ['<Dataset>','</Dataset>']));
output(dataset(prefix+'toxml4.utf8', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%)));
output(dataset(prefix+'toxml5.utf8', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%)));
output(dataset(prefix+'toxml6.utf8', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%)));
output(dataset(prefix+'toxml7.utf8', { unicode line{maxlength(4096)} }, csv(unicode), HINT(layoutTranslation, %translationmode%)));
);
