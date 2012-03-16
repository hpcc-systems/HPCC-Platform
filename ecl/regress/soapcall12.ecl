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

//Test all the different variants of SOAPCALL to ensure they get processed correctly

GetAttributeInRecord := RECORD
string                ModuleName{xpath('ModuleName')};
string                AttributeName{xpath('AttributeName')};
unsigned              Version{xpath('Version')};
boolean               GetSandbox{xpath('GetSandbox')};
boolean               GetText{xpath('GetText')};
                 END;


GetAttributeOutRecord := RECORD
string                ModuleName{xpath('ModuleName')};
string                text{xpath('Text')};
                END;


GetAttributeInRecord createInRecord := TRANSFORM
            SELF.ModuleName := 'SearchModule';
            SELF.AttributeName := 'SearchAttribute';
            SELF.Version := 0;
            SELF.GetSandbox := true;
            SELF.GetText := true;
        END;

GetAttributeInRecord createInRecord2(string moduleName, string attr) := TRANSFORM
            SELF.ModuleName := moduleName;
            SELF.AttributeName := attr;
            SELF.Version := 0;
            SELF.GetSandbox := true;
            SELF.GetText := true;
        END;

GetAttributeInRecord2 := RECORD
string                ModuleName{xpath('ModuleName')} := 'MyModule';
string                AttributeName{xpath('AttributeName')} := 'MyAttribute';
unsigned              Version{xpath('Version')} := 1;
boolean               GetSandbox{xpath('GetSandbox')} := true;
boolean               GetText{xpath('GetText')} := true;
                 END;

ds := dataset([{'doxie','One'},{'jimbo','Two'},{'yankie','Three'}],{string moduleName, string attr});

//--No argument, record provides values--
//actions
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2, LOG(MIN));
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2, RETRY(100), TIMEOUT(99), LOG('Call abc'));


output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr), dataset(GetAttributeOutRecord), LOG('megacorp:'+LEFT.moduleName+'.'+LEFT.attr)));

output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr), dataset(GetAttributeOutRecord), LOG,LOG(MIN),LOG('megacorp:'+moduleName+'.'+attr)));


output(HTTPCALL('http://webservices.megacorp.com', 'WsAttributesDs', 'blah', GetAttributeOutRecord, LOG,LOG(MIN),LOG('megacorp:xxx')));
