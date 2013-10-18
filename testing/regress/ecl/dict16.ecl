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

idRec := { unsigned id; };

inRecord := RECORD
  unsigned search;
  DATASET(idRec) ids;
END;

inRecord processIds(inRecord l) := TRANSFORM
  myDict := DICTIONARY([{l.search-1},{l.search},{l.search+1}], idRec);
  SELF.ids := l.ids(id IN myDict);
  SELF := l;
END;

ds := NOFOLD(DATASET([
    {1,[{2},{3},{4}]},
    {3,[{2},{3},{4}]},
    {10,[{4},{8},{11}, {10}, {7}]},
    {11,[{4},{8},{11}, {10}, {7}]}], inRecord));
    
output(PROJECT(ds, processIds(LEFT)));
