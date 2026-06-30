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

//Test searching for 'foo' in a dataset
//This test demonstrates the search functionality with the term 'foo'

import Std.Str;

searchTerm := 'foo' : STORED('searchTerm');

// Create a test dataset with various strings, some containing 'foo'
testDataRec := RECORD
    UNSIGNED4 id;
    STRING20 value;
END;

testData := DATASET([
    {1, 'foo'},
    {2, 'bar'},  
    {3, 'foobar'},
    {4, 'barfoo'},
    {5, 'foobaz'},
    {6, 'hello'},
    {7, 'world'},
    {8, 'footprint'},
    {9, 'roof'},
    {10, 'food'}
], testDataRec);

// Search for records containing 'foo'
searchResults := testData(REGEXFIND('foo', value));

// Alternative search using position (proper way)
positionResults := testData(Str.Find(value, searchTerm, 1) > 0);

// Count results
fooCount := COUNT(searchResults);
positionCount := COUNT(positionResults);

// Output results
OUTPUT(searchResults, NAMED('SearchResults'));
OUTPUT(positionResults, NAMED('PositionResults')); 
OUTPUT(fooCount, NAMED('FooCount'));
OUTPUT(positionCount, NAMED('PositionCount'));

// Test that both methods give same results
OUTPUT(fooCount = positionCount, NAMED('SameResultCount'));