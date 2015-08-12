//##############################################################################
//
//    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
############################################################################## */



//proxypath := 
//myname := 
//proxyname := 'dispatch';
//proxyname := 'log';

url := proxypath + 'soap' + proxyname + '.cgi';

ns := 'urn:TestSOAP/TestService';

namerec := {STRING10 name := myname};

greetrec := RECORD,MAXLENGTH(100)
    STRING salutation{xpath('greetingResponse/salutation')};
    UNSIGNED4 time{xpath('_call_latency')};
END;

greeting := SOAPCALL(url, 'greeting', namerec, greetrec, LITERAL, NAMESPACE(ns));

listset := DATASET([{'cat dog pig'}, {'hello world'}], {STRING11 list});

espsplitrec := RECORD,MAXLENGTH(100)
    STRING args{xpath('item')};
    UNSIGNED4 time{xpath('_call_latency')};
END;

espsplit := SOAPCALL(listset, url, 'espsplit', {listset.list}, DATASET(espsplitrec), NAMESPACE(ns));

//OUTPUT(greeting);
//OUTPUT(espsplit);
