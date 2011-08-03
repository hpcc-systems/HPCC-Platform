/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
