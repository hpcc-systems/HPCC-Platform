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



m := module
export boolean a:= true;
export boolean b := false;
export boolean c := true;
end;

i := interface
export boolean a;
export boolean b;
export boolean c;
export boolean d;
end;

arg := module(project(m, i, opt))
export boolean d := false;
end;

output(arg.a);
output(arg.b);
output(arg.c);
output(arg.d);
