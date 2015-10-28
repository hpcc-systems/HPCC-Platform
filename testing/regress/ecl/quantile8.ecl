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

//version scale=32
//version scale=0x400
//version scale=0x10000
//version scale=0x100000
//version scale=0x1000000
//xversion scale=0x10000000

import ^ as root;
scale := #IFDEFINED(root.scale, 1024);

rawRec := { REAL id; };

quantRec := RECORD(rawRec)
    UNSIGNED4 quant;
END;

rawRec createRaw(real id) := TRANSFORM
    SELF.id := id;
END;

quantRec createQuantile(rawRec l, UNSIGNED quant) := TRANSFORM
    SELF := l;
    SELF.quant := quant;
END;

calcId(integer cnt, integer delta, unsigned c) := FUNCTION
    prime := 181;
    x1 := (c-1)*prime;
    x2 := x1 % cnt;
    x3 := (integer)x2;
    x4 := (integer)(x3 - delta);
    x5 := x4 / delta;
    RETURN x5;
END;
    
createDataset(integer cnt, integer delta) := FUNCTION
    RETURN DATASET(cnt, createRaw(calcId(cnt, delta, COUNTER)), DISTRIBUTED);
END;

inDs := createDataset(scale*2, scale);
output(QUANTILE(inDs, 64, { id }, RANGE([2,62])));
