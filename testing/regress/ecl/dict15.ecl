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
      STRING SoFar;
      STRING Rest;
    END;
    
    Initial := DATASET([{'',searchWord}],R);
    
    R Pluck1(DATASET(R) infile) := FUNCTION
      R TakeOne(R le, UNSIGNED1 c) := TRANSFORM
        SELF.SoFar := le.SoFar + le.Rest[c];
        SELF.Rest := le.Rest[..c-1]+le.Rest[c+1..]; // Boundary Conditions handled automatically
      END;
      RETURN NORMALIZE(infile,LENGTH(LEFT.Rest),TakeOne(LEFT,COUNTER));
    END;
    
    Processed := LOOP(Initial,LENGTH(searchWord),Pluck1(ROWS(LEFT)));
    
    Anagrams := TABLE(Processed, { string word := SoFar; });
    
    RETURN Anagrams(Word in knownWords);
END;

wordIndex := TextSearch.getWordIndex(multiPart, false);
allWordsDs := DEDUP(SORTED(wordIndex), word);
knownWords := DICTIONARY(allWordsDs, { word });

string searchWord := 'gabs' : stored('word');

OUTPUT(search(searchWord, knownWords));
