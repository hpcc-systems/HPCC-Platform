/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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


rec := {unsigned id};
ds := DATASET(10, TRANSFORM(rec, SELF.id := HASH(COUNTER))) : independent(few);


doSomething(rec l) := FUNCTION

    doit := OUTPUT(DATASET(l),,NAMED('results'), EXTEND);
    doit2 := WHEN(doit, OUTPUT(DATASET([{0}],rec),,NAMED('done'), EXTEND), SUCCESS);
    doit3 := WHEN(doit2, OUTPUT(DATASET([{1}],rec),,NAMED('done'), EXTEND), FAILURE);
    RETURN doit3;
END;


NOTHOR(APPLY(ds, doSomething(ROW(ds))));
