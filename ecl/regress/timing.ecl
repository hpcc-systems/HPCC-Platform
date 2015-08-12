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

rtl := SERVICE
unsigned4 msTick() : eclrtl,library='eclrtl',c,entrypoint='rtlTick';
END;

DoThing1() := output('hello');
DoThing2() := output('hello again');

unsigned4 StartTime := rtl.mstick() : independent;

DoThing1();
output(rtl.mstick()-StartTime,NAMED('TIME1'));
DoThing2();
output(rtl.mstick()-StartTime-WORKUNIT('TIME1', unsigned4),NAMED('TIME2'));
