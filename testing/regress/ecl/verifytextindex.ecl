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

//version multiPart=false,variant='default'
//version multiPart=false,variant='inplace'
//version multiPart=false,variant='inplace_row'
//version multiPart=false,variant='inplace_lzw'
//version multiPart=false,variant='inplace_lz4hc'
//version multiPart=true,variant='inplace'

#onwarning (4523, ignore);

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, 'inplace');

//--- end of version configuration ---

import $.setup;
files := setup.files(multiPart, false);

compareFiles(ds1, ds2) := FUNCTIONMACRO
    c := COMBINE(ds1, ds2, transform({ boolean same, RECORDOF(LEFT) l, RECORDOF(RIGHT) r,  }, SELF.same := LEFT = RIGHT; SELF.l := LEFT; SELF.r := RIGHT ), LOCAL);
    RETURN output(choosen(c(not same), 10));
ENDMACRO;

other := files.getSearchIndexVariant(variant);

original := files.getSearchIndex();

compareFiles(original, other);
