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

SHARED rec := RECORD
  INTEGER1 num;
END;

export Interface_Def := INTERFACE
    EXPORT DATASET(rec) Records;
END;

ds1 := DATASET([{1},{2},{3}],rec);

context1 := MODULE(Interface_Def)
    EXPORT DATASET(rec) Records := ds1;
END;

OUTPUT(context1.Records,NAMED('context1'));

export Interface_Def2 := INTERFACE
    EXPORT GROUPED DATASET(rec) Records;
END;

ds2 := GROUP(SORTED(DATASET([{1},{2},{3}],rec),num),num);

// ----- This will not syntax check
// ----------- Gives error "Error: Explicit type for Records doesn't match definitin in base module (30,2)"
// ------------------ code 2346
context2 := MODULE(Interface_Def2)
    EXPORT GROUPED DATASET(rec) Records := ds2;
END;

OUTPUT(context2.Records,NAMED('context2'));
