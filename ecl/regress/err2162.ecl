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

aaa := DATASET('aaa', {STRING1 fa}, hole);
bbb := DATASET('bbb', {STRING1 fb}, hole, aaa);
OUTPUT(GROUP(aaa, GROUP(bbb, fb)[1].fb));


/*
aaa := DATASET('aaa', {STRING1 fa, STRING1 fb}, hole);
bbb := DATASET('aaa', {STRING1 fa, STRING1 fb}, hole, aaa);

NestedGroup := Group(aaa, Group(bbb,bbb.fa)[1].fa);

GroupedSet := Group(aaa, aaa.fa);

SecondSort := SORT(GroupedSet, aaa.fb);

// This gives: an Unhandled operator in SqlSelect::gatherSelects()
Output(SecondSort);

MyTable := TABLE(SecondSort, {aaa.fa, aaa.fb});

// This gives: assert false in hqlexpr.cpp
OUTPUT(Mytable);
*/
