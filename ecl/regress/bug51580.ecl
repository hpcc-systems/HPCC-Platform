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

SHARED Layout := RECORD
    STRING10 w;
END;

SHARED Foo := INTERFACE
    EXPORT STRING name;
    EXPORT DATASET(Layout) ds;
END;

SHARED Bar := MODULE(Foo)  // problem when inherited interface specified
    EXPORT STRING name := 'THOR:;PERSIST::BAR';
    d1 := DATASET([{'aa'}, {'bb'}], Layout);
    EXPORT DATASET(Layout) ds := d1 : PERSIST(name);
END;

output(Bar.ds);
