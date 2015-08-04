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

export svc1 :=
    SERVICE
        echo(integer4 value) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;

d := dataset('f::f', {big_endian integer4 f, integer4 g}, thor);

output(sort(d, f),,'g::g');

output(d(f + (big_endian integer4)3 > (big_endian integer4)0),,'g::g2');


apply(d, svc1.echo(f), svc1.echo(g));