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

//Comprehensive test for searching "foo" using various methods
//This demonstrates multiple ways to search for 'foo' in the HPCC Platform

import Std.Str;

// Test dataset with various string patterns
dataRec := RECORD
    UNSIGNED4 id;
    STRING20 text;
    STRING10 category;
END;

sampleData := DATASET([
    {1, 'foo', 'exact'},
    {2, 'FOO', 'uppercase'},
    {3, 'Foo', 'capitalized'},
    {4, 'foobar', 'prefix'},
    {5, 'barfoo', 'suffix'},
    {6, 'barfoobaz', 'middle'},
    {7, 'hello world', 'none'},
    {8, 'food for thought', 'contains'},
    {9, 'roof', 'contains'},
    {10, 'foolish', 'prefix'},
    {11, '', 'empty'},
    {12, 'foofoo', 'double'}
], dataRec);

// Test 1: Exact match (case sensitive)
exactMatch := sampleData(text = 'foo');

// Test 2: Case insensitive search  
caseInsensitiveMatch := sampleData(Str.ToLowerCase(text) = 'foo');

// Test 3: Contains 'foo' (case sensitive)
containsMatch := sampleData(Str.Find(text, 'foo', 1) > 0);

// Test 4: Contains 'foo' (case insensitive)
containsMatchInsensitive := sampleData(Str.Find(Str.ToLowerCase(text), 'foo', 1) > 0);

// Test 5: Regex search for 'foo'
regexMatch := sampleData(REGEXFIND('foo', text));

// Test 6: Starts with 'foo'
startsWithMatch := sampleData(Str.Find(text, 'foo', 1) = 1);

// Test 7: Ends with 'foo'
endsWithMatch := sampleData(text[LENGTH(text)-2..] = 'foo');

// Test 8: Word boundary search (foo as complete word)
wordBoundaryMatch := sampleData(REGEXFIND('\\bfoo\\b', text));

// Output all results
OUTPUT(exactMatch, NAMED('ExactMatch'));
OUTPUT(caseInsensitiveMatch, NAMED('CaseInsensitiveMatch'));
OUTPUT(containsMatch, NAMED('ContainsMatch'));
OUTPUT(containsMatchInsensitive, NAMED('ContainsMatchInsensitive'));
OUTPUT(regexMatch, NAMED('RegexMatch'));
OUTPUT(startsWithMatch, NAMED('StartsWithMatch'));
OUTPUT(endsWithMatch, NAMED('EndsWithMatch'));
OUTPUT(wordBoundaryMatch, NAMED('WordBoundaryMatch'));

// Summary counts
OUTPUT(COUNT(exactMatch), NAMED('ExactMatchCount'));
OUTPUT(COUNT(containsMatch), NAMED('ContainsMatchCount'));
OUTPUT(COUNT(regexMatch), NAMED('RegexMatchCount'));

// Validation: regex and contains should give same results for this simple case
OUTPUT(COUNT(regexMatch) = COUNT(containsMatch), NAMED('RegexContainsEqual'));