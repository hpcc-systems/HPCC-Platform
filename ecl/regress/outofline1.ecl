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

//The following are work arounds until out of line functions are properly implemented
//There are here just to test the function parameters could be generated correctly
#option ('hoistSimpleGlobal', false);
#option ('workunitTemporaries', false);


idRec := { unsigned id };

countDataset1(dataset(idRec) x) := DEFINE NOTHOR(COUNT(x(id > 0)));
countDataset2(linkcounted dataset(idRec) x) := DEFINE NOTHOR(COUNT(x(id > 1)));

x1 := DATASET([1,2,3], idRec);
output(countDataset1(x1));
output(countDataset2(x1));
