/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

IMPORT * FROM lib_redis.sync;

STRING servers := '--SERVER=127.0.0.1:6379';
FlushDB(servers);

SetBoolean(servers, 'b', TRUE);
GetBoolean(servers, 'b');

REAL pi := 3.14159265359;
SetReal(servers, 'pi', pi);
GetReal(servers, 'pi');

REAL pi2 := 3.14159265359*2;
SetReal(servers, 'pi', pi2, 1);
GetReal(servers, 'pi', 1);

INTEGER i := 123456789;
SetInteger(servers, 'i', i);
GetInteger(servers, 'i');

UNSIGNED u := 7;
SetUnsigned(servers, 'u', u);
GetUnsigned(servers, 'u');

STRING str  := 'supercalifragilisticexpialidocious';
SetString(servers, 'str', str);
GetString(servers, 'str');

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SetUnicode(servers, 'uni', uni);
GetUnicode(servers, 'uni');

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SetUtf8(servers, 'utf8', utf);
GetUtf8(servers, 'utf8');

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
SetData(servers, 'data', mydata);
GetData(servers,'data');

Exists(servers, 'utf8');
Del(servers, 'utf8');
Expire(servers, 'str', 1); 
Persist(servers, 'str');
//The following test some exceptions
GetInteger(servers, 'pi');

NOFOLD(DBSize(servers));
NOFOLD(DBSize(servers, 1));
NOFOLD(DBSize(servers, 2));
FlushDB(servers);
NOFOLD(Exists(servers, 'utf8'));
