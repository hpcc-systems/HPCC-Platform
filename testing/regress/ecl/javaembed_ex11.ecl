/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

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

IMPORT Java;

UNSIGNED JavaAccumulator(INTEGER initial) := EMBED(Java : persist('Global'))
public class JavaAccumulator
{
  public JavaAccumulator(int initial) { tot = initial; }
  public synchronized int accumulate(int a)
  {
    tot = tot + a;
    return tot;
  }
  public synchronized int clear()
  {
    int _tot = tot;
    tot = 0;
    return _tot;
  }
  private int tot = 0;
}
ENDEMBED;

INTEGER accumulate(UNSIGNED p, INTEGER val) := IMPORT(Java, 'JavaAccumulator::accumulate');
INTEGER clear(UNSIGNED p) := IMPORT(Java, 'JavaAccumulator::clear');

a := JavaAccumulator(35) : INDEPENDENT;

ORDERED
(
  accumulate(a, 1);
  accumulate(a, 2);
  accumulate(a, 3);
  clear(a);
  accumulate(a, 10);
);
