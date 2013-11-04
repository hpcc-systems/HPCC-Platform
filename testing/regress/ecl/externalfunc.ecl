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

inRec := { unsigned id };
doneRec := { unsigned4 execid };
out1rec := { unsigned id; };
out2rec := { real id; };

unsigned4 count1(DATASET(inRec) input) := BEGINC++
  return lenInput / sizeof(unsigned __int64);
ENDC++;

unsigned4 count2(_LINKCOUNTED_ DATASET(inRec) input) := BEGINC++
  return countInput;
ENDC++;

unsigned4 count3(_ARRAY_ DATASET(inRec) input) := BEGINC++
  return countInput;
ENDC++;


ds1 := DATASET([1,2,3,4,5],inRec);
output(count1(ds1));
output(count2(ds1));
output(count3(ds1));
