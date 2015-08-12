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


inputRecord := record
string20            surname;
string20            forename;
                end;


lookupRecord := record
string20            surname;
string20            forename;
unsigned            frequency;
                end;

outputRecord := record
string20            surname;
string20            forename;
unsigned8           score;
                end;


IAbcHelper := interface
export createRecord(inputRecord l, lookupRecord r) := transform(outputRecord, self := []);
export processInput(dataset(inputRecord) inFile) := inFile;
export processOutput(dataset(outputRecord) inFile) := inFile;
export boolean useKey;
        end;


lookupFile := dataset('lookup', lookupRecord, thor);
lookupKey := index(lookupFile, { lookupFile }, 'lookupIndex');

getScoredDataset(IAbcHelper helper, dataset(inputRecord) inFile) := function

    input := helper.processInput(inFile);

    joinToFile := join(input, lookupFile, left.surname = right.surname and left.forename = right.forename, helper.createRecord(LEFT, RIGHT));
    joinToKey := join(input, lookupKey, left.surname = right.surname and left.forename = right.forename, helper.createRecord(LEFT, ROW(RIGHT, lookupRecord)));
    joinedResult := if(helper.useKey, joinToKey, joinToFile);

    return helper.processOutput(joinedResult);
end;


GavinAbcHelper := module(IAbcHelper)
export createRecord(inputRecord l, lookupRecord r) := transform(outputRecord, self := l, self.score := 100000 DIV r.frequency);
export processInput(dataset(inputRecord) inFile) := inFile(surname != '');
export processOutput(dataset(outputRecord) inFile) := inFile(score != 0);
export boolean useKey := false;
        end;


ds := dataset('ds', inputRecord, thor);

output(getScoredDataset(GavinAbcHelper, ds));
