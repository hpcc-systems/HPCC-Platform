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
namesRecord := RECORD
    STRING name1;
    STRING10 name2;
    DATASET(childrec) childnames;
    DICTIONARY(childrec) childdict;
    unsigned1 val1;
    integer1   val2;
    UTF8 u1;
    UNICODE u2;
    UNICODE8 u3;
    BIG_ENDIAN unsigned6 val3;
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

STREAMED dataset(namesRecord) streamedNames(data d, utf8 u) := EMBED(Python)
  return [  \
     ("Gavin", "Halliday", [("a", 1)], 250, -1,  U'là',  U'là',  U'là', 1234566, d, False, {"1","2"}), \
     ("John", "Smith", [], 250, -1,  U'là',  U'là',  u, 1234566, d, True, [])]
ENDEMBED;

output(streamedNames(d'AA', u'là'));
