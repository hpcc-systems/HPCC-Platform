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
IMPORT Java;

unsigned persister(integer initial) := EMBED(Java : persist('workunit'))
public class persister
{
  public persister(int initial) { tot = initial; }
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

integer accumulate(unsigned p, integer val) := IMPORT(Java, 'accumulate');
integer clear(unsigned p) := IMPORT(Java, 'clear');
release(unsigned p) := IMPORT(Java, '~persister'); // After calling this the java object p is no longer usable

p := persister(35) : global;

  accumulate(p, 1);
  accumulate(p, 2);
  accumulate(p, 3);
  clear(p);
  accumulate(p, 10);
  