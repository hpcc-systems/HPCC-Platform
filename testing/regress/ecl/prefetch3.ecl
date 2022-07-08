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

//class=file
//version multiPart=true
//nokey

rtl := SERVICE
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick',volatile;
 unsigned4 sleep(unsigned4 _delay) : eclrtl,action,library='eclrtl',entrypoint='rtlSleep';
END;

//--- end of version configuration ---

slowInput(unsigned numRows, unsigned delay) := DATASET(numRows, TRANSFORM({ unsigned id, unsigned tick }, SELF.id := COUNTER + rtl.sleep(delay); SELF.tick := rtl.msTick()));


result0 := slowInput(10, 20) + slowInput(20, 10);           // threaded concat
result1 := slowInput(10, 20) & slowInput(20, 10);           // ordered concat
result2 := PREFETCH(slowInput(10, 20)) & PREFETCH(slowInput(20, 10));
result3 := PREFETCH(slowInput(10, 20),PARALLEL) & PREFETCH(slowInput(20, 10), PARALLEL);
result4 := PREFETCH(slowInput(10, 20),30,PARALLEL) & PREFETCH(slowInput(20, 10),30,PARALLEL);

summarise(ds) := FUNCTIONMACRO
  summary := TABLE(NOFOLD(ds), { unsigned minTick := MIN(GROUP, tick); unsigned maxTick := MAX(GROUP, tick) });
  Result := summary[1].maxTick - summary[1].minTick;
  RETURN Result;
ENDMACRO;

//Expected time for sequential execution is 380ms, parallel should be ~190ms.
sequential(
    summarise(result0),
    summarise(result1),
    summarise(result2),
    summarise(result3),
    summarise(result4),
)
