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