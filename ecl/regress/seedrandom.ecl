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


rtl := service
setSeed(unsigned4 seed) :                       eclrtl,library='eclrtl',entrypoint='rtlSeedRandom';
end;

r := { unsigned4 id };

ds := dataset([{0}], r);
normal := normalize(ds, CLUSTERSIZE, transform(r, self.id := counter));
dist := distribute(normal, id);

apply(dist, rtl.setSeed(123456));
