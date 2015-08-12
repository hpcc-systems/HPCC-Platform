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

#option ('globalFold', false);
//this returns false

bingo_phonecalls1(string phone) :=
     phone in
['2012899613',
 '2012899645',
 '498942000000'];


bingo_phonecalls1('2012899613');

//but this returns true

bingo_phonecalls2(string phone) :=
     phone in
['2012899613',
// '2012899645',    //only change is commenting out this line
 '498942000000'];


bingo_phonecalls2('2012899613');

//and this returns true

bingo_phonecalls3(string10 phone) := //only change from first example is implicit string length
     phone in
['2012899613',
 '2012899645',
 '498942000000'];


bingo_phonecalls3('2012899613');
