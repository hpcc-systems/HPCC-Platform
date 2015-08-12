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

rec := record
       integer1 v;
     end;

l := dataset([{1},{2},{3}], rec);
r := dataset([{1},{3},{4}], rec);

rec t(rec ll, rec rr) := TRANSFORM
    SELF := ll;
  END;

j1 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), LEFT ONLY);
output(j1);
j2 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), LEFT OUTER);
output(j2);
j3 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), RIGHT ONLY);
output(j3);
j4 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), RIGHT OUTER);
output(j4);
j5 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT));
output(j5);
j6 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), FULL OUTER);
output(j6);
