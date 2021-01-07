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

//class=embedded
//class=embedded-js
//class=3rdparty

//nothor

IMPORT javascript;

integer add1(integer val) := EMBED(javascript) val+1; ENDEMBED;
string add2(string val) := EMBED(javascript) val+'1'; ENDEMBED;
string add3(varstring val) := EMBED(javascript) val+'1'; ENDEMBED;
utf8 add4(utf8 val) := EMBED(javascript) val+'1'; ENDEMBED;
unicode add5(unicode val) := EMBED(javascript, U' val+\' at Oh là là Straße\';');

data testData(data val) := EMBED(javascript) val[0] = val[0] + 1; val; ENDEMBED;
set of integer testSet(set of integer val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of unsigned2 testSet0(set of unsigned2 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of string testSet2(set of string val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of string testSet3(set of string8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring testSet4(set of varstring val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring8 testSet5(set of varstring8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of boolean testSet6(set of boolean val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real4 testSet7(set of real4 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real8 testSet8(set of real8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of integer2 testSet9(set of integer2 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

r := RECORD
    UNSIGNED id;
    STRING name;
END;

m(unsigned numRows, boolean isLocal = false, unsigned numParallel = 0) := MODULE
  EXPORT streamed dataset(r) myDataset(unsigned numRows = numRows) := EMBED(javascript : activity, local(isLocal), parallel(numParallel))
    var numSlaves = __activity__.numSlaves;
    var numParallel = numSlaves * __activity__.numStrands;
    var rowsPerPart = (numRows + numParallel - 1) / numParallel;
    var thisSlave = __activity__.slave;
    var thisIndex = thisSlave * __activity__.numStrands + __activity__.strand;
    var first = thisIndex * rowsPerPart;
    var last = first + rowsPerPart;
    if (first > numRows)
        first = numRows;
    if (last > numRows)
        last = numRows;
  
    var names = [ "Gavin", "Richard", "John", "Bart" ];
    var ds = [ ];
    while (first < last)
    {
        ds.push( { id : first, name: names[first % 4] });
        first += 1;
    }
    ds;
  ENDEMBED;
END;

add1(10);
add2('Hello');
add3('World');
add4(U'Oh là là Straße');
add5(U'Стоял');

add2('Oh là là Straße');  // Passing latin chars - should be untranslated

testdata(D'aa');
testSet([1,2,3]);
testSet0([30000,40000,50000]);
testSet2(['one','two','three']);
testSet3(['uno','dos','tre']);
testSet4(['un','deux','trois']);
testSet5(['ein','zwei','drei']);
testSet6([false,true,false,true]);
testSet7([1.1,2.2,3.3]);
testSet8([1.2,2.3,3.4]);
testSet9([-111,0,113]);

s1 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
SUM(NOFOLD(s1 + s2), a);

s1a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add2((STRING)COUNTER)));
s2a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add3((STRING)(COUNTER/2))));
SUM(NOFOLD(s1a + s2a), a);

s1b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
SUM(NOFOLD(s1b + s2b), a);

s1c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING) COUNTER + '1')));
s2c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING)(COUNTER/2) + '1')));
SUM(NOFOLD(s1c + s2c), a);

// Test embed activity

//Global activity - fixed number of rows
output(m(10).myDataset());
//Local version of the activity 
output(count(m(10, isLocal := true).myDataset()) = CLUSTERSIZE * 10);

//Check that stranding (if implemented) still generates unique records
output(COUNT(DEDUP(m(1000, numParallel := 5).myDataset(), id, ALL)));

r2 := RECORD
    UNSIGNED id;
    DATASET(r) child;
END;

//Check that the activity can also be executed in a child query
output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := m(COUNTER).myDataset())));

//Test stranding inside a child query
output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := m(COUNTER, NumParallel := 3).myDataset())));
