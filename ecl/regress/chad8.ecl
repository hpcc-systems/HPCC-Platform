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

rec := record
   unsigned4 pad1;
   unsigned1 score_field;
   unsigned8 pad2;
   unsigned6 did_field;
   string63  pad3;
   string10  phone_field;
   end;

infile := dataset('x', rec, thor);

__didfilter__ := infile.did_field = 0 or infile.score_field <> 100;
__go__ := length(trim((string)(integer)infile.phone_field))=10 and (__didfilter__);

__804__ := __go__;



y := infile(~__804__);
output(y);



