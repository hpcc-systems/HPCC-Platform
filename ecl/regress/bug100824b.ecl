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

vmod := module, virtual
end;

mymod := module(vmod) // syntax error
// mymod := module // no syntax error, runs as expected

  shared layout := {
    unsigned u1,
    unsigned u2,
  };

  shared ds1 := dataset([{1,2},{3,4}], layout);
  shared key2 := index(ds1, {u1}, {u2}, '~dustin::delete::key2');

  export build2 := buildindex(key2, overwrite);
end;

sequential(
  mymod.build2
);
