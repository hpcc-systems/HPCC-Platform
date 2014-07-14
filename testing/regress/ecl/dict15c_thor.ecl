/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//Find all anagrams of a word, that match the list of known words
import $.Common;
import $.Common.TextSearch;

wordIndex := TextSearch.getWordIndex('thorlcr', false);
allWordsDs := DEDUP(wordIndex, word, HASH);
knownWords := DICTIONARY(allWordsDs, { word });

shortWords := TABLE(allWordsDs, { Word })(LENGTH(TRIM(Word)) <= 6); 

//Find all words that have anagrams 
//BUG: Without the NOFOLD the code generator merges the projects, and introduces an ambiguous dataset
withAnagrams := TABLE(NOFOLD(shortWords), { word, anagrams := Common.Dict15c(Word, knownWords) });

moreThanOne := withAnagrams(count(anagrams)>1);

OUTPUT(sort(moreThanOne, RECORD));
