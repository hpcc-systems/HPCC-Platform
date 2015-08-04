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

ppersonRecord := RECORD
string3 id := '000';
string10 surname := '';
string10 forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
END;

tmptable := dataset([
{'001', 'Hawthorn','Gavin', 13, 10},
{'002', 'Smith','Zack', 13, 10},
{'003', 'Hewit','Nigel', 13, 10},
{'004', 'Gillin','Paul', 13, 10}], ppersonRecord);


s0 := DEDUP(tmptable, forename, ALL);


s1 := SORT(tmptable, surname) : STORED('jaketest_stored11', few);

OUTPUT(s1, , 'out1.d00', OVERWRITE);

