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


rec := record
     REAL r;
     DECIMAL5 d;
     QSTRING7 q;
     UNICODE9 u;
     VARSTRING v;
       end;

outrec := record
        unsigned8 h;
      end;

ds := DATASET([{1.2,2.2,'333','444','555'}],rec);

output(ds);

outrec T(rec L, rec r) := transform
    self.h := hash64(L.r);
end;

output(iterate(ds,T(LEFT, right)));
