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

xstring := type
    export string1 load( string1 x) := x;
    export string1 store(string1 x) := x;
end;

r := RECORD
  xstring extra;
END;

ds := DATASET('test',r,FLAT);

// this is ok
//count(ds);

// this causes mem leak
output(ds,,'out.d00');

