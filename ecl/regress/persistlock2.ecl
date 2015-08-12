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

rtl := service
unsigned4 delayReturn(unsigned4 value, unsigned4 sleepTime) : eclrtl,entrypoint='rtlDelayReturn';
        end;

unsigned4 waitTime := 1000;

p1 := 1 : persist('p1');

p2 := rtl.delayReturn(p1,waitTime) : persist('p2');
p3 := rtl.delayReturn(p2,waitTime) : persist('p3');
p4 := rtl.delayReturn(p3,waitTime) : persist('p4');
p5 := rtl.delayReturn(p4,waitTime) : persist('p5');
p6 := rtl.delayReturn(p5,waitTime) : persist('p6');
p7 := rtl.delayReturn(p6,waitTime) : persist('p7');
p8 := rtl.delayReturn(p7,waitTime) : persist('p8');
p9 := rtl.delayReturn(p8,waitTime) : persist('p9');

output(p9);
