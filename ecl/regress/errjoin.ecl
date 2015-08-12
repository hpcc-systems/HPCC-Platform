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


// reduce the size, so we can test quickly.

Person1 := Choosen(Person, 20);

//MySet := SORT(Person1, -Person1.per_last_name, -Person1.per_first_name);

MySet := SORT(SORT(Person1, -Person1.per_last_name), -Person1.per_first_name);

OUTPUT(MySet, {Person1.per_last_name, Person1.per_first_name});
