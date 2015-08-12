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



checkIn(inval, searchSet = '[]') := macro
    (inVal = 0 or inVal in searchSet)
    endmacro;

mylist1 := [1,2,3];
mylist2 := [1,2,100];

searchValue := 100;
checkIn(100, mylist1);
checkIn(100, mylist2);
checkIn(100);

checkIn(searchValue, mylist1);
checkIn(searchValue, mylist2);
checkIn(searchValue);
