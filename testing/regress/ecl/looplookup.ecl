/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

//nohthor TBD

rec := RECORD  
 unsigned id;
END;

rhs := DATASET(10000, TRANSFORM(rec, SELF.id := COUNTER) , DISTRIBUTED) : INDEPENDENT;
lhs := DATASET([{1}, {2}, {3}], rec);


loopBody(DATASET(rec) currentlhs) := JOIN(currentlhs, rhs, LEFT.id=RIGHT.id, TRANSFORM(rec, SELF.id := RIGHT.id+1), SMART, HINT(lookupRhsConstant(true)));
doloop := LOOP(lhs, 200, loopBody(ROWS(LEFT)));

doloop;
