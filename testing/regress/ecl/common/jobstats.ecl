/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

 
EXPORT JobStats := MODULE

  import ^ as root;
  import Std.File;
  import lib_WorkunitServices.WorkunitServices;
  import $.^.setup;

  Files := Setup.Files(true, false);
  SHARED wordIndex := Files.getWordIndex();

  EXPORT jobStatKindRec := RECORD
   string stat;
  END;

  EXPORT getStats(string wuid, string filter, DATASET(jobStatKindRec) statKinds) := FUNCTION
    stats := NOTHOR(WorkunitServices.WorkunitStatistics(wuid, false, filter));
    j := JOIN(stats, statKinds, LEFT.name = RIGHT.stat);
    s := SORT(j, name);
    RETURN TABLE(s, {name, value});
  END;

  SHARED searchWords := TABLE(wordIndex, {word}) : INDEPENDENT; // so that in a different workflow item from kj, to isolate jhtree cache usage

  EXPORT TestKJSimple(boolean forceRemote = false, boolean forceLocal = false) := FUNCTION
    RETURN IF(forceRemote,
             JOIN(searchWords, wordIndex, RIGHT.kind = 1 AND LEFT.word = RIGHT.word, HINT(forceRemoteKeyedLookup(true))),
             IF(forceLocal,
                JOIN(searchWords, wordIndex, RIGHT.kind = 1 AND LEFT.word = RIGHT.word, HINT(remoteKeyedLookup(false))),
                JOIN(searchWords, wordIndex, RIGHT.kind = 1 AND LEFT.word = RIGHT.word)
               )
            );
  END;

  EXPORT TestKJCQ(boolean forceRemote = false, boolean forceLocal = false) := FUNCTION
    rec := RECORD
      unsigned1 kind;
      unsigned1 wip;
      unsigned8 wPosSum := 0;
    END;

    rec cqtrans(rec l) := TRANSFORM
      kj := IF(forceRemote,
               JOIN(searchWords, wordIndex, RIGHT.kind = l.kind AND LEFT.word = RIGHT.word AND l.wip = RIGHT.wip, HINT(forceRemoteKeyedLookup(true))),
               IF(forceLocal,
                  JOIN(searchWords, wordIndex, RIGHT.kind = l.kind AND LEFT.word = RIGHT.word AND l.wip = RIGHT.wip, HINT(remoteKeyedLookup(false))),
                  JOIN(searchWords, wordIndex, RIGHT.kind = l.kind AND LEFT.word = RIGHT.word AND l.wip = RIGHT.wip)
                 )
              );
      SELF.wPosSum := SUM(kj, wpos);
      SELF := l;
    END;
 
    lhsRecs := 10;
    lhs := DATASET(lhsRecs, TRANSFORM(rec, SELF.kind := 1; SELF.wip := COUNTER), DISTRIBUTED) + DATASET(lhsRecs, TRANSFORM(rec, SELF.kind := 2; SELF.wip := COUNTER), DISTRIBUTED);
    RETURN PROJECT(lhs, cqtrans(LEFT));
  END;
END;
