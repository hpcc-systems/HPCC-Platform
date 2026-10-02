/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

trec := record
         unsigned4 seq;
         unsigned4 strm;
         unsigned4 nodenum;
         unsigned4 key;
       end;

seed1 := dataset([{0, 1, 0, 0}], trec);
seed2 := dataset([{0, 2, 0, 0}], trec);
seed3 := dataset([{0, 3, 0, 0}], trec);

seeds := [seed1, seed2, seed3];

m := MERGE(seeds, SORTED(seq, strm % 20));
output(m);
