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

r := RECORD
  UNSIGNED4 id;
  STRING50 name;
  STRING100 address;
END;

r2 := RECORD(r)
  STRING extra{maxlength(1000)}
END;

ds := dataset('ds', r, thor);

ASSERT(RANDOM() != 0, CONST);

ASSERT(SIZEOF(R) = 154);
ASSERT(SIZEOF(R2,MAX) = 1158);
ASSERT(SIZEOF(R) = 154, CONST);
ASSERT(SIZEOF(R) = 154, CONST);


//Not supported as a generate time constant - too many complexities with ifblocks/var length etc
ASSERT(SIZEOF(ds) = 154);
ASSERT(SIZEOF(recordof(ds)) = 154, const);  // but this is
