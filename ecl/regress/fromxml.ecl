/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
    u8'<Row><surname>Hawthorn</surname><forename>Abigäil</forename><homephone areaCode="09876" number="123987"/><hasmobile>false</hasmobile><contact hasEmail="false"><phone areaCode="" number="0"/></contact><books><Row><title>The cat in the hat</title><author>Suess</author></Row><Row><title>Wolly the sheep</title><author></author></Row></books><colours><Item>Red</Item><Item>Yellow</Item></colours><endmarker>$$</endmarker></Row>'
    ], { utf8 text; });

output(namesTable, { FROMXML(personRecord, text, TRIM); });
output(1);