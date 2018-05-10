/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

//class=embedded
//class=python2

IMPORT Python;

r := RECORD
    UNSIGNED id;
    STRING name;
END;

m(unsigned numRows, boolean isLocal = false, unsigned numParallel = 0) := MODULE
  EXPORT streamed dataset(r) myDataset(unsigned numRows = numRows) := EMBED(Python : activity, local(isLocal), parallel(numParallel))
    numSlaves = __activity__.numSlaves
    numParallel = numSlaves * __activity__.numStrands
    rowsPerPart = (numRows + numParallel - 1) / numParallel
    thisSlave = __activity__.slave
    thisIndex = thisSlave * __activity__.numStrands + __activity__.strand
    first = thisIndex * rowsPerPart
    last = first + rowsPerPart
    if first > numRows:
      first = numRows
    if last > numRows:
      last = numRows

    names = [ "Gavin", "Richard", "John", "Bart" ]
    while first < last:
        yield (first, names[first % 4 ])
        first += 1
  ENDEMBED;
END;

r2 := RECORD
    UNSIGNED id;
    DATASET(r) child;
END;

sequential(
  //Global activity - fixed number of rows
  output(m(10).myDataset());

  //Local version of the activity 
  output(count(m(10, isLocal := true).myDataset()) = CLUSTERSIZE * 10);

  //Check that stranding (if implemented) still generates unique records
  output(COUNT(DEDUP(m(1000, numParallel := 5).myDataset(), id, ALL)));

  //Check that the activity can also be executed in a child query
  output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := m(COUNTER).myDataset())));

  //Test stranding inside a child query
  output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := m(COUNTER, NumParallel := 3).myDataset())));
);