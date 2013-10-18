/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    u8'<Row><surname>Hälliday</surname><forename>Gavin</forename><homephone areaCode="09876" number="123987"/><hasmobile>true</hasmobile><mobilephone areaCode="07967" number="123987"/>' +
        u8'<contact hasEmail="true"><phone areaCode="n/a" number="0"/><email>gavin@edata.com</email></contact>' + 
        u8'<books><Row><title>εν αρχη ην ο λογος</title><author>john</author></Row><Row><title>To kill a mocking bird</title><author>Lee</author></Row><Row><title>Zen and the art of motorcycle maintainence</title><author>Pirsig</author></Row></books><colours><All/></colours><endmarker>$$</endmarker></Row>',
    u8'<Row><surname>Halliday</surname><forename>Abigäil</forename><homephone areaCode="09876" number="123987"/><hasmobile>false</hasmobile><contact hasEmail="false"><phone areaCode="" number="0"/></contact><books><Row><title>The cat in the hat</title><author>Suess</author></Row><Row><title>Wolly the sheep</title><author></author></Row></books><colours><Item>Red</Item><Item>Yellow</Item></colours><endmarker>$$</endmarker></Row>'
    ], { utf8 text; });

output(namesTable, { FROMXML(personRecord, text, TRIM); });
output(1);