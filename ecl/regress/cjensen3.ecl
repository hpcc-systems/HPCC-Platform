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

integer4 basemyint := 99;
integer4 myInt := if(exists(dataset(workunit('isrecovering'), {boolean flag})), 1, basemyint);

myrec := RECORD
            string30 addr;
END;

myDS := dataset([{'359 Very Rocky River Dr'}],myrec);

outrec := if(myint<2,myDS,FAIL(myrec,99,'ouch')) : RECOVERY(output(dataset([true],{boolean flag}),,named('isRecovering')));

output(count(outrec),NAMED('Count'));

output(outrec,NAMED('Outrec'));


