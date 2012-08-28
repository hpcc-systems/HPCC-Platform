/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

makeDataset(set of integer x) := dataset(x, { integer value; });

isUnique1(set of integer x) := not exists(makeDataset(x)(value in x));

isUnique2(set of integer x) := not exists(join(makeDataset(x),makeDataset(x),left.value=right.value,all));

set of integer values := [1,2,3,4,5,6,3,2,7] : stored('values');


output(isUnique1(values));
output(isUnique2(values));
