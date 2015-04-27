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

//version multiPart=false
//version multiPart=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

#onwarning(4523, ignore);

//Find all anagrams of a word, that match the list of known words
import $.Common.TextSearch;
import $.setup.TS;

search(string searchWord, DICTIONARY({TS.wordType word}) knownWords) := FUNCTION

  R := RECORD
    STRING Word;
  END;

  trimmedWord := TRIM(searchWord);
  Initial := DATASET([{trimmedWord}],R);
    
  R Pluck1(DATASET(R) infile, unsigned4 numDone) := FUNCTION
    R TakeOne(R le, UNSIGNED1 c) := TRANSFORM
      SELF.Word := le.Word[1..numDone] + le.Word[c] + le.Word[numDone+1..c-1]+le.Word[c+1..]; // Boundary Conditions handled automatically
    END;
    RETURN NORMALIZE(infile,LENGTH(LEFT.Word)-numDone,TakeOne(LEFT,numDone+COUNTER));
  END;
    
  anagrams := LOOP(Initial,LENGTH(trimmedWord),Pluck1(ROWS(LEFT),COUNTER-1));
   
  uniqueAnagrams := DEDUP(anagrams, Word, HASH);
  RETURN uniqueAnagrams(Word in knownWords);
END;

wordIndex := TextSearch.getWordIndex(multiPart, false);
allWordsDs := DEDUP(wordIndex, word, HASH);
knownWords := DICTIONARY(allWordsDs, { word });

shortWords := TABLE(allWordsDs, { Word })(LENGTH(TRIM(Word)) <= 6); 

//Find all words that have anagrams 
//BUG: Without the NOFOLD the code generator merges the projects, and introduces an ambiguous dataset
withAnagrams := TABLE(NOFOLD(shortWords), { word, anagrams := search(Word, knownWords) });

moreThanOne := withAnagrams(count(anagrams)>1);

OUTPUT(sort(moreThanOne, RECORD));
