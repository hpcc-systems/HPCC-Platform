/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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


//Test weird dataset options e.g., a count for the number of entries, with an ifblock inside the nested dataset.  Yuk!

nestedRecord := RECORD
    boolean hasForename;
    boolean hasSurname;
    IFBLOCK (SELF.hasForename)
        STRING forename;
    END;
    IFBLOCK (SELF.hasSurname)
        STRING surname;
    END;
END;

mainRecord := RECORD
    UNSIGNED id;
    UNSIGNED numNested;
    DATASET(nestedRecord, COUNT(SELF.numNested)) nestedRecord;
END;

mainTable := dataset([
        {1, 1, [{true, true, 'Gavin', 'Hawthorn'}] },
        {2, 2, [{false, false}, { true, false, 'X' } ] },
        {3, 3, [{true, false, 'Jim'}, { false, true, 'Jones' }, { false, false }]},
        { 99, 0, []}], mainRecord);

saved1 := mainTable : independent(many);

saved2 := saved1 : independent(few);

saved3 := saved2 : independent(many);

saved4 := TABLE(saved3, { saved3, unsigned forceTransform := 0; }) : independent(few);

saved5 := TABLE(saved4, { saved4 } - [forceTransform]);

output(saved5);


//test that the same record type embedded twice in the same record resolves the child fields correctly

baseRecord := RECORD
    UNSIGNED id;
    nestedRecord first;
    nestedRecord second;
END;

baseTable := dataset([
        {1, {true, true, 'Gavin', 'Hawthorn'}, { false, false } },
        {2, {false, false}, { true, false, 'X' } },
        {3, {true, false, 'Jim'}, { false, true, 'Jones' } },
        {99, {false, false}, {false, false} }], baseRecord);

bsaved1 := baseTable : independent(many);

bsaved2 := bsaved1 : independent(few);

bsaved3 := bsaved2 : independent(many);

bsaved4 := TABLE(bsaved3, { bsaved3, unsigned forceTransform := 0; }) : independent(few);

bsaved5 := TABLE(bsaved4, { bsaved4 } - [forceTransform]);

output(bsaved5);
