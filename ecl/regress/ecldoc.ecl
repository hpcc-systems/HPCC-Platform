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

#option ('generateLogicalGraph', true);

/**
 * Defines a record that contains information about a person
 */
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

/**
Defines a table that can be used to read the information from the file
and then do something with it.
 */

namesTable := dataset('x',namesRecord,FLAT);


/**
    Allows the name table to be filtered.

    @param  ages    The ages that are allowed to be processed.
            badForename Forname to avoid.

    @return         the filtered dataset.
 */

namesTable filtered2(set of integer2 ages, string badForename) := namesTable(age in ages, forename != badForename);

output(filtered2([10,20,33], ''));
