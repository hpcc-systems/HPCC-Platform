/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems.

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

//class=embedded
//class=3rdparty
//nohthor

import java;

integer accumulate(integer a) := EMBED(Java: persist('thread'))
public class persisty
{
  public persisty()
  {
  }
  public synchronized int accumulate(int a)
  {
    tot = tot + a;
    return tot;
  }
  private int tot = 0;
}
ENDEMBED;

r := record
  integer i;
end;

r t(r l) := TRANSFORM
  SELF.i := accumulate(l.i);
END;

d1 := DATASET([{1}, {2}, {3}], r);
d2 := DATASET([{3}, {4}, {5}], r);

accumulated := PROJECT(d1, t(LEFT))+PROJECT(d2, t(LEFT));
max(accumulated, i) = 3+4+5;  // The order cannot be predicted but the max should be consistent.
