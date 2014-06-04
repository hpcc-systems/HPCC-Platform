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

//Find all anagrams of a word, that match the list of known words

import $.^.setup.TS;

export dict15b(string searchWord, DICTIONARY({TS.wordType word}) knownWords) := FUNCTION

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
