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

xx := dataset('ff', {string20 per_last_name, unsigned8 holepos}, thor);

ds := xx(per_last_name='Hawthorn');
pagesize := 100 : stored('pagesize');
fpos := 0 : stored('fpos');

result := choosen(ds(holepos >= fpos), pagesize);

//evaluate(result[pagesize], xx.holepos) : stored('lpos');
output(result);

