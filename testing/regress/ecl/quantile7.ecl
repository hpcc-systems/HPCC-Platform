/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

//Temporarily disable tests on hthor/thor, until activity is implemented
//nohthor
//nothor

//version scale=10
//version scale=100
//version scale=1000
//version scale=10000
//version scale=100000
//version scale=1000000
//version scale=10000000

import ^ as root;
scale := #IFDEFINED(root.scale, 1000000);

rawRec := { unsigned id; };

quantRec := RECORD(rawRec)
    UNSIGNED4 quant;
END;

rawRec createRaw(UNSIGNED id) := TRANSFORM
    SELF.id := id;
END;

quantRec createQuantile(rawRec l, UNSIGNED quant) := TRANSFORM
    SELF := l;
    SELF.quant := quant;
END;

createDataset(unsigned cnt, integer delta) := FUNCTION
    prime := 181;
    RETURN DATASET(cnt, createRaw(((COUNTER-1) * prime) % cnt + delta), DISTRIBUTED);
END;

midPoint := 1000000000000000000;
inDs := createDataset(scale*2+1, midPoint - scale);
output(QUANTILE(inDs, 2, { id }));
