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
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2);
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2, RETRY(100), TIMEOUT(99));

//Rows
results := SOAPCALL('http://webservices.megacorp.com', 'WsAttributesRow', GetAttributeInRecord2, GetAttributeOutRecord);
output('Text is ' + results.text);
results2 := SOAPCALL('http://webservices.megacorp.com', 'WsAttributesRow', GetAttributeInRecord2, GetAttributeOutRecord, RETRY(100), TIMEOUT(99));
output('Text is ' + results2.text);

//Dataset
output(SOAPCALL('http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord2, dataset(GetAttributeOutRecord)));
output(SOAPCALL('http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord2, dataset(GetAttributeOutRecord), RETRY(100), TIMEOUT(99)));

//--No argument, transform provides values--
//actions
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord, createInRecord2('GavinModule','MainAttr'));
SOAPCALL('http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord, createInRecord, RETRY(100), TIMEOUT(99));

//Rows
results3 := SOAPCALL('http://webservices.megacorp.com', 'WsAttributesRow', GetAttributeInRecord, createInRecord, GetAttributeOutRecord);
output('Text is ' + results3.text);
results4 := SOAPCALL('http://webservices.megacorp.com', 'WsAttributesRow', GetAttributeInRecord, createInRecord, GetAttributeOutRecord, RETRY(100), TIMEOUT(99));
output('Text is ' + results4.text);

//Dataset
output(SOAPCALL('http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord, dataset(GetAttributeOutRecord)));
output(SOAPCALL('http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord, dataset(GetAttributeOutRecord), RETRY(100), TIMEOUT(99)));

//--Argument, record provides values--

SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2);
SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord2, RETRY(100), TIMEOUT(99));

//Dataset
output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord2, dataset(GetAttributeOutRecord)));
output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord2, dataset(GetAttributeOutRecord), RETRY(100), TIMEOUT(99)));

//--Argument, transform provides values--
//actions
SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr));
SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributes', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr), RETRY(100), TIMEOUT(99));

//Dataset
output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr), dataset(GetAttributeOutRecord)));
output(SOAPCALL(ds, 'http://webservices.megacorp.com', 'WsAttributesDs', GetAttributeInRecord, createInRecord2(LEFT.moduleName, LEFT.attr), dataset(GetAttributeOutRecord), RETRY(100), TIMEOUT(99),RESPONSE(NOTRIM)));

