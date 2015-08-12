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

#option ('targetClusterType','thor');
#option ('pickBestEngine', 0);

myrecord := record
 real diff;
 integer1 reason;
  end;

rms5008 := 10;
rms5009 := 11;
rms5010 := 12;
rms5011 := 13;

btable :=
dataset([{rms5008,72},{rms5009,7},{rms5010,30},{rms5011,31}],myrecord);

RCodes := sort(btable,-btable.diff);

RCode1 := RCodes[1].reason;
RCode2 := RCodes[2].reason;

RCode1;
RCode2;

// use evaluate
RCode1x := evaluate(RCodes[1],RCodes.reason);
RCode2x := evaluate(RCodes[2],RCodes.reason);
RCode1x;
Rcode2x;

RCodes[10].reason;

