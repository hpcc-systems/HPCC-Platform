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
    string20  name;
    unsigned4 sequence;
    unsigned4 value;
  END;

seed100 := dataset([{'',1,0},{'',2,0},{'',3,0},{'',4,0},{'',5,0},{'',6,0},
{'',7,0},{'',8,0},{'',9,0},
                    {'',1,1},{'',2,1},{'',3,1},{'',4,1},
{'',5,1},{'',6,1},{'',7,1},{'',8,1},{'',9,1},
                    {'',1,2},{'',2,2},{'',3,2},{'',4,2},
{'',5,2},{'',6,2},{'',7,2},{'',8,2},{'',9,2},
                    {'',1,3},{'',2,3},{'',3,3},{'',4,3},
{'',5,3},{'',6,3},{'',7,3},{'',8,3},{'',9,3},
                    {'',1,4},{'',2,4},{'',3,4},{'',4,4},
{'',5,4},{'',6,4},{'',7,4},{'',8,4},{'',9,4}
                   ], rec);

sortedseed100 :=  sort(seed100 ,sequence);

output(DEDUP(sortedseed100, sequence, ALL, LOCAL));
