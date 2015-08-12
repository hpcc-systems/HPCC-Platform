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

inputFile := dataset('input', {string9 ssn, string9 did, string20 addr},thor);


keyedFile := dataset('keyed', {string9 ssn, string9 did, string20 surname, string20 forename}, thor, encrypt('myKey'));
SsnKey := INDEX(keyedFile, {ssn,unsigned8 fpos {virtual(fileposition)}}, '~index.ssn');
DidKey := INDEX(keyedFile, {did,unsigned8 fpos {virtual(fileposition)}}, '~index.did');

filledRec :=   RECORD
        string9 ssn;
        string9 did;
        string20 addr;
        string20 surname;
        string20 forename;
    END;

filledRec getNames(inputFile l, keyedFile r) := TRANSFORM
        SELF := l;
        SELF := r;
    END;

KeyedTable := keyed(keyedFile, SsnKey, DidKey);

FilledRecs := join(inputFile, KeyedTable,left.did=right.did,getNames(left,right), KEYED(DidKey));
output(FilledRecs);

