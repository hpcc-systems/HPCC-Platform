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

#option ('foldAssign', false);
#option ('globalFold', false);
sname := type
  export integer loadlength(string9 s) := 9;
  export integer load(string9 s) := (integer)s;
  export string9 store(integer s) := (string9)s;
  end;

LayoutDirectTV := record
 sname   qid;
 string1 dummy
end;
testdata := dataset ('temp6', LayoutDirectTV, flat);

output(testdata, {(integer)qid+(integer)('1'+'2')});
