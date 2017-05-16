/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems.

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

zero := 0 : stored('zero');
d := dataset([
        {0},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9},
        {1},{1},{2},{3},{4},{8},{9}
        ], { unsigned r; }, DISTRIBUTED);
p2 := TABLE(NOFOLD(d), { COUNT(group) }, LOCAL);
copies := IF(__PLATFORM__='roxie',1,CLUSTERSIZE);
output(count(p2) = copies);
