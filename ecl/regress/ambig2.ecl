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

didRecord := record
unsigned8       did;
            end;

rec := record
unsigned8       did;
dataset(didRecord) x;
unsigned8       filepos{virtual(fileposition)};
        end;


ds := dataset('ds', rec, thor);


rec t(rec l) := transform
    unsigned6 did := (unsigned6)l.did;
z := dataset([{did},{did}], didRecord);
    grz := group(z, row);
    SELF.x := group(sort(dedup(grz, did),did));
    SELF := l;
    end;


p := project(ds, t(left));


output(p);
