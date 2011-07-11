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

phoneRecord := 
            RECORD
string5         areaCode;
string12        number;
            END;

contactrecord := 
            RECORD
phoneRecord     phone;
boolean         hasemail;
                ifblock(self.hasemail)
string              email;
                end;
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
string2         endmarker := '$$';
            END;

namesTable2 := dataset([
    
    {'Halliday','Gavin','09876','123987',true,'07967','123987', 'n/a','n/a',
true,'gavin@edata.com'},
        {'Halliday','Abigail','09876','123987',false,'','',false}
        ], personRecord);

output(namesTable2);