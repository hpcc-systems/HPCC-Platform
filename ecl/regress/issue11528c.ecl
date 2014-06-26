/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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


d1 := DATASET([1,2,3,4,56,31,1324,134], { unsigned id });

p1 := d1 : persist('~p1', single);

addMessage := output(dataset(['Hi'], { string text }),named('msgs'), EXTEND);

allMessages := DATASET(WORKUNIT('msgs'), { string text });

p2 := p1(id != 10) : persist('pzzz', single, FEW);


import Std.File;

doFail := FAIL('Fail because this is being run for the first time!');

cleanUp := IF(count(allMessages)=1, doFail);

x1 := WHEN(p2, addMessage, BEFORE);

output(WHEN(x1, cleanup, BEFORE),named('results'));
