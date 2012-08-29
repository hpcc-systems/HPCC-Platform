/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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



decimal17_4 sum1 := 20;
unsigned count1 := 3;
output((decimal20_10)(sum1/count1));


decimal17_4 sum2 := 20 : stored('sum2');
unsigned count2 := 3 : stored('count2');
output((decimal20_10)(sum2/count2));

rec := { decimal17_4 sumx, unsigned countx };
ds := dataset([{20,3},{10,2},{10.0001,2}], rec);
output(nofold(ds), { sumx, countx, decimal20_10 average := sumx/countx, sumx between 10 and 10.00009, sumx between 10D and 10.00009D });


decimal17_4 value1 := 1.6667;
decimal17_4 value2 := 1.6667 : stored('value2');

output(round(value1));
output(roundup((real)value1));

output(round((real)value2));
output(roundup((real)value2));
