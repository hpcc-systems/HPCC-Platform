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

storedTrue := true : STORED('storedTrue');
msgRec := { unsigned4 code, string text; };

boolean isOdd(unsigned4 x) := (x & 1)=1;

ds1 := NOFOLD(DATASET([1,2,3,4,5,6,7], { INTEGER id }));
successMsg := nofold(dataset([{0,'Success'}], msgRec));
failureMsg(unsigned4 code) := nofold(dataset([{code,'Failure'}], msgRec));

when1 := WHEN(ds1, output(successMsg,NAMED('Messages'),EXTEND), success);

output(when1(isOdd(id)));

oCond := IF(storedTrue, output(failureMsg(10),NAMED('Messages'),EXTEND), output('Incorrect'));

when2 := WHEN(ds1, oCond, success);

output(when2(not isOdd(id)));
