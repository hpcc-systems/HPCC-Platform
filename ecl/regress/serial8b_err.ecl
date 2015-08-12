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

//Test that dictionaries are correctly serialized to nonlocal child queries
// Should get a type incompatibility error on the dataset assignment

#option ('pickBestEngine', false);

IMPORT SerialTest;

interestingWords := DICTIONARY([{'elves'},{'cheddar'}], SerialTest.wordRec);

inDs := SerialTest.libraryDatasetFile;

filteredBooks(string searchName, dictionary(SerialTest.wordRec) searchWords) := 
            SerialTest.bookIndex(
                KEYED(title = searchName), 
                EXISTS(words(word IN searchWords))
            );


p := TABLE(inDs, { owner, DATASET(SerialTest.bookDsrec) books := filteredBooks(books[1].title, interestingWords) } );

output(p);
