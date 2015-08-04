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

RETURN MODULE

export z(inf, outf) := macro

gr := group(inf, age);

outf := table(gr, {count(gr)});

endmacro;

export z2(inf) := functionmacro

dataset gr := group(inf, age);

return table(gr, {count(gr)});

endmacro;


export ageSplit(inf) := functionmacro

    result := module
        export young := inf(age < 18);
        export old := inf(age > 70);
        export others := inf(age between 18 and 70);
    end;
    return result;
endmacro;

END;
