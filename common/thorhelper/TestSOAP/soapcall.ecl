//##############################################################################
//
//    Copyright (C) 2011 HPCC Systems.
//
//    All rights reserved. This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Affero General Public License as
//    published by the Free Software Foundation, either version 3 of the
//    License, or (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU Affero General Public License for more details.
//
//    You should have received a copy of the GNU Affero General Public License
//    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
