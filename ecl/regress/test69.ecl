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

tRec1 :=
    record
        String15 K := '!';
        String9 N := '0';
        String1 EOL := x'0a';
    end;

tRec1 createSeqNum(tRec1 L, tRec1 R) :=
  transform
    self.K := R.K;
    self.N := (string9)(((integer4)L.N)+1);
    self.EOL := L.EOL
  end;

DS1 := dataset ('test01', tRec1, flat);

IT := iterate(DS1, createSeqNum(left, right));

output (IT ,tRec1,'testit.out');


