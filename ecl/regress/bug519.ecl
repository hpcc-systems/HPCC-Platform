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

string isEmpty(set of integer a) := IF ( a = [], 'yes','no' );
string isOneTwoThree(set of integer a) := IF ( a = [1,2,3], 'yes','no' );
string aboveOneTwo(set of integer a) := IF ( a > [1,2], 'yes','no' );
string aboveOneThousandEtc(set of integer1 a) := IF ( a > [1000,1001], 'yes','no' );
string biggerThanMe(set of string a) := IF ( a > ['Gavin','Hawthorn'], 'yes','no' );

isEmpty([1,2]);
isEmpty([]);
isOneTwoThree([1,2]);
isOneTwoThree([1,2,3]);
isOneTwoThree([1,2,3,4]);
aboveOneTwo([1]);
aboveOneTwo([1,2]);
aboveOneTwo([1,2,3]);
aboveOneThousandEtc([1,2,3]);
biggerThanMe(['Richard','Drimbad']);
