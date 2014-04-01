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

idRecord := { unsigned id; };

makeDataset(unsigned start) := dataset(3, transform(idRecord, self.id := start + counter));


zero := 0 : stored('zero');
one := 1 : stored('one');
two := 2 : stored('two');
three := 3 : stored('three');
five := 5 : stored('five');


c0 := CHOOSE(zero, makeDataset(1), makeDataset(2), makeDataset(3));
c1 := CHOOSE(one, makeDataset(1), makeDataset(2), makeDataset(3));
c2 := CHOOSE(two, makeDataset(1), makeDataset(2), makeDataset(3));
c3 := CHOOSE(three, makeDataset(1), makeDataset(2), makeDataset(3));
c5 := CHOOSE(five, makeDataset(1), makeDataset(2), makeDataset(3));

sequential(
    output(c0),
    output(c1),
    output(c2);
    output(c3),
    output(c5);
    );
