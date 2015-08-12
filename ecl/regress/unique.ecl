/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    This program is free software: you can redistribute it and/or modify
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
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick';
 unsigned4 sleep(unsigned4 _delay) : eclrtl,library='eclrtl',entrypoint='rtlSleep';
END;


x := _INSTANCE_(rtl.msTick());
y := rtl.msTick();

evaluate(x); rtl.Sleep(10); output(y - x);
