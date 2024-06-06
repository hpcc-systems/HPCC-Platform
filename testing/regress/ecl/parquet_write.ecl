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

//class=parquet
//nothor,noroxie

IMPORT Parquet;
IMPORT Std;

SimpleRecord := RECORD
    INTEGER id;
    STRING name;
    DECIMAL8 price;
    BOOLEAN isActive;
END;

STRING generateName(INTEGER id) := CHOOSE(id % 10 + 1, 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J');

smallDataset := DATASET(50, TRANSFORM(SimpleRecord,
    SELF.id := COUNTER,
    SELF.name := generateName(COUNTER),
    SELF.price := 10.00, // Fixed price
    SELF.isActive := COUNTER % 2 = 0 // Alternating boolean values
));

mediumDataset := DATASET(250, TRANSFORM(SimpleRecord,
    SELF.id := COUNTER,
    SELF.name := generateName(COUNTER),
    SELF.price := 20.00, // Fixed price
    SELF.isActive := COUNTER % 2 = 0 // Alternating boolean values
));

ParquetIO.Write(smallDataset, '/var/lib/HPCCSystems/mydropzone/small1.parquet', TRUE);
ParquetIO.Write(mediumDataset, '/var/lib/HPCCSystems/mydropzone/medium1.parquet', TRUE);

smallReadbackData := ParquetIO.Read(SimpleRecord, '/var/lib/HPCCSystems/mydropzone/small1.parquet');
mediumReadbackData := ParquetIO.Read(SimpleRecord, '/var/lib/HPCCSystems/mydropzone/medium1.parquet');

OUTPUT(smallReadbackData, NAMED('SmallDataset'));
OUTPUT(mediumReadbackData, NAMED('MediumDataset'));
