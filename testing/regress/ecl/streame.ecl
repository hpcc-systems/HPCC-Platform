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

//class=embedded

IMPORT Python;

childrec := RECORD
   string name => unsigned value;
END;

namesRecord := RECORD
    STRING name1;
    STRING10 name2;
    LINKCOUNTED DATASET(childrec) childnames;
    LINKCOUNTED DICTIONARY(childrec) childdict{linkcounted};
    childrec r;
    unsigned1 val1;
    integer1   val2;
    UTF8 u1;
    UNICODE u2;
    UNICODE8 u3;
    BIG_ENDIAN unsigned4 val3;
    DATA d;
    BOOLEAN b;
    SET OF STRING ss1;
END;

dataset(namesRecord) blockedNames(string prefix) := EMBED(Python)
  return ["Gavin","John","Bart"]
ENDEMBED;

_linkcounted_ dataset(namesRecord) linkedNames(string prefix) := EMBED(Python)
  return ["Gavin","John","Bart"]
ENDEMBED;

dataset(namesRecord) streamedNames(data d, utf8 u) := EMBED(Python)
  return [  \
     ("Gavin", "Halliday", [("a", 1),("b", 2),("c", 3)], [("aa", 11)], ("aaa", 111), 250, -1,  U'là',  U'là',  U'là', 0x01000000, d, False, set(["1","2"])), \
     ("John", "Smith", [], [], ("c", 3), 250, -1,  U'là',  U'là',  u, 0x02000000, d, True, []) \
     ]
ENDEMBED;

// Test use of Python generator object for lazy evaluation...

dataset(childrec) testGenerator(unsigned lim) := EMBED(Python)
  num = 0
  while num < lim:
    yield ("Generate:", num)
    num += 1
ENDEMBED;

output(streamedNames(d'AA', u'là'));
output (testGenerator(10));

// Test what happens when two threads pull from a generator
c := testGenerator(1000);
count(c(value < 500));
count(c(value > 500));

// Test Python code returning named tuples
childrec tnamed(string s) := EMBED(Python)
  import collections;
  childrec = collections.namedtuple("childrec", "value,name")
  return childrec(1,s)
ENDEMBED;

output(tnamed('Yo').name);
