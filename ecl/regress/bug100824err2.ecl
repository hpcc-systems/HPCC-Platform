/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

Layout := {
  unsigned id,
};

Model := module, virtual
  dataset(layout) GetInvalid := dataset([], layout);
end;

DocInfo := module(Model) // no syntax error; no runtime error; bad results

  export dsLni := dataset([{1},{3}], layout);
  export dskey:= dataset([{2},{3}], layout);
  export keyDocId := index(dskey, {id}, {dskey}, '~dustin::delete::docId');

  export GetInvalid
    := join(dsLni, keyDocId,
            keyed(left.id = right.id),
            transform(right),
            right only);
end;

sequential(
  buildindex(DocInfo.keyDocid),
  output(DocInfo.GetInvalid)
);
