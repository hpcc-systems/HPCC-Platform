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



IXyz := interface

    export string name := '';
    export getId(unsigned id, unsigned x) := (id * 100);
end;

Mxyz := module(IXyz)
    export getId(unsigned id, unsigned x) := (id * 200);
end;


f(IXyz in) := in.getId(5,3) + in.getId(10,3);

output(f(Mxyz));

