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
  string1 n1x;
  string1 n2;
  string1 n3x;
  string1 n4;
  string1 n5x;
  string1 n6;
end;

dd := dataset('dd',rec,flat);

r1 := record  string1 n1x;  string1 n2; string1 n3x;  string1 n4; end;
r2 := record  string1 n1x;  string1 n2; string1 n5x;  string1 n6; end;

dataset f(virtual dataset(r1) d) := d(n1x>'A',n2>'B',n3x='C',n4='D');

dataset g(virtual dataset(r2) d) := d(n1x<'E',n2<'F',n5x='G',n6='H');

dataset h(virtual dataset d) := g(f(d));

output(h(dd));
