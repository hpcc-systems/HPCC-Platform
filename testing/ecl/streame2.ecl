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

IMPORT Python;

childrec := RECORD
   string name => unsigned value;
END;

namerec := RECORD
   string name;
END;

// Test use of Python generator object for lazy evaluation...

dataset(childrec) testGenerator(unsigned lim) := EMBED(Python)
  num = 0
  while num < lim:
    yield ("Generate:", num)
    num += 1
ENDEMBED;

// Test use of Python named tuple...

dataset(childrec) testNamedTuple(unsigned lim) := EMBED(Python)
  import collections
  ChildRec = collections.namedtuple("childrec", "value, name") # Note - order is reverse of childrec - but works as we get fields by name
  c1 = ChildRec(1, "name1")
  c2 = ChildRec(name="name2", value=2)
  return [ c1, c2 ]
ENDEMBED;

// Test 'missing tuple' case...

dataset(namerec) testMissingTuple1(unsigned lim) := EMBED(Python)
  return [ '1', '2', '3' ]
ENDEMBED;

dataset(namerec) testMissingTuple2(unsigned lim) := EMBED(Python)
  return [ ('1'), ('2'), ('3') ]
ENDEMBED;

//output (testGenerator(10));
output (testNamedTuple(10));
//output (testMissingTuple1(10));
//output (testMissingTuple2(10));