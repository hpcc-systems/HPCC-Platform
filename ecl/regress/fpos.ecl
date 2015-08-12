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

xRecord := RECORD
data9 per_cid;
    END;

yRecord := RECORD
xRecord;
unsigned8 __filepos{VIRTUAL(FILEPOSITION)}
    END;

aRecord := RECORD
unsigned8 __filepos{VIRTUAL(FILEPOSITION)};
xRecord;
    END;

xDataset := DATASET('x',xRecord,FLAT);
output(xDataset,, 'outx.d00');

yDataset := DATASET('y',yRecord,FLAT);
output(yDataset,{per_cid,__filepos}, 'outy.d00');

output(yDataset,,'outz.d00');

aDataset := DATASET('a',aRecord,FLAT);
output(aDataset,, 'outa.d00');
