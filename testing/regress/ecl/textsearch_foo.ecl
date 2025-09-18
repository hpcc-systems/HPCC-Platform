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

//nothor
//version multiPart=false
//version multiPart=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useLocal := #IFDEFINED(root.useLocal, false);

//--- end of version configuration ---

#option ('checkAsserts',false);
#onwarning (3164, ignore);

import $.Common.TextSearch;

// Test queries that search for "foo"
fooQueries := DATASET([
    {'"foo"'},                              // Simple search for foo
    {'OR("foo", "bar")'},                   // Search for foo OR bar  
    {'AND("foo", "bar")'},                  // Search for foo AND bar (both must exist)
    {'PHRASE("foo", "bar")'},               // Search for phrase "foo bar"
], TextSearch.queryInputRecord);

// Execute the search queries
output(TextSearch.executeBatchAgainstWordIndex(fooQueries, useLocal, multiPart, 0x00000200));