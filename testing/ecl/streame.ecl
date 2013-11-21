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

IMPORT Python;

namesRecord := RECORD
    STRING name;
END;

dataset(namesRecord) blockedNames(string prefix) := EMBED(Python)
  return ["Gavin","John","Bart"]
ENDEMBED;

_linkcounted_ dataset(namesRecord) linkedNames(string prefix) := EMBED(Python)
  return ["Gavin","John","Bart"]
ENDEMBED;

streamed dataset(namesRecord) streamedNames(string prefix) := EMBED(Python)
  return ["Gavin","John","Bart"]
ENDEMBED;

titles := dataset(['', 'Mr. ', 'Rev. '], { string title });

//output(normalize(titles, blockedNames(left.title), transform(right)));
//output(normalize(titles, linkedNames(left.title), transform(right)));
//output(normalize(titles, streamedNames(left.title), transform(right)));
output(streamedNames('mr'));
