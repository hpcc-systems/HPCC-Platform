/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
//class=python2

IMPORT Python;

nested := RECORD
  SET OF integer value;
END;

parent := RECORD
  DATASET(nested) nest;
END;

DATASET(parent) getP() := EMBED(C++)
  __result = rtlMalloc(46);
  __lenResult = 46;
  memcpy(__result, "\x2a\x00\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00"
"\x00\x00\x10\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00", 46);
ENDEMBED;

unsigned pcode(DATASET(parent) p) := EMBED(Python)
  count = 0
  for child in p:
   for c2 in child.nest:
     for c3 in c2:
       for c4 in c3:
         count += c4

  return count
ENDEMBED;

ds := getp();
ds2 := DATASET([{[{[1,2]},{[3,4]}]}], parent);
sequential(
    output(ds),
    output(ds2),
    pcode(ds);
    pcode(ds2);
    ''
);
