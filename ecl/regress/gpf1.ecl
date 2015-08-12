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

reject_rec := record
   integer4 SequenceKey;
   integer1 rejectreason;
   string151 Acct_No;
end;

R08649L__R08649L_rejects := dataset('gbtest', reject_rec, flat);

output(sort(R08649L__R08649L_rejects,rejectreason),gbtest2.out)

ta := table(R08649L__R08649L_rejects, {rejectreason,count(group)},rejectreason);

output(ta,,'gbtest.out');
