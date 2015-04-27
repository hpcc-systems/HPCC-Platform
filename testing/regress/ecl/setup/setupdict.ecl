/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import $.SerialTest;

//******************************** Dictionary dataset creation code ***********************

libraryDs := DATASET([
    { 'gavin',
        [{'the hobbit',
            [{'gandalf'},{'rivendell'},{'dragon'},{'dwarves'},{'elves'}]},
         {'eragon',
            [{'eragon'},{'dragon'},{'spine'},{'elves'},{'dwarves'},{'krull'}]}
        ]},
    { 'jim',
        [{'complete diy',
            [{'heating'},{'electrics'},{'nuclear reactors'},{'spaceships'}]},
        {'cheeses',
            [{'cheddar'},{'parmesan'},{'stilton'},{'wensleydale'}]}
        ]}], SerialTest.libraryDsRec);
        
libraryDictDs := DATASET([
    { 'gavin' =>
        [{'the hobbit' =>
            [{'gandalf'},{'rivendell'},{'dragon'},{'dwarves'},{'elves'}]},
         {'eragon' =>
            [{'eragon'},{'dragon'},{'spine'},{'elves'},{'dwarves'},{'krull'}]}
        ]},
    { 'jim' =>
        [{'complete diy' =>
            [{'heating'},{'electrics'},{'nuclear reactors'},{'spaceships'}]},
        {'cheeses' =>
            [{'cheddar'},{'parmesan'},{'stilton'},{'wensleydale'}]}
        ]}], SerialTest.libraryDictRec);
        
OUTPUT(libraryDs,,SerialTest.DsFilename,OVERWRITE);

OUTPUT(libraryDictDs,,SerialTest.DictFilename,OVERWRITE);

allBooks := SerialTest.libraryDatasetFile.books;

createBookIndex := INDEX(allBooks, { string20 title := title }, { dataset(SerialTest.wordRec) words := words }, SerialTest.BookKeyFilename);

BUILD(createBookIndex, overwrite);
