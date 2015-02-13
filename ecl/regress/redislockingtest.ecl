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

IMPORT * FROM lib_redis.locking;
IMPORT FlushDB FROM lib_redis.sync;

STRING servers := '--SERVER=127.0.0.1:6379';

//SetBoolean(servers, 'b', TRUE);
//GetBoolean(servers, 'b');

UNSIGNED lock := GetLockObject(servers, 'pi');
//GetLockObject(servers, 'pi');
//GetLockObject(servers, 'pi');
NOFOLD(MissThenLock(lock));
NOFOLD(MissThenLock(lock));


REAL pi := 3.14159265359;
//OUTPUT(IF( MissThenLock(lock), SetReal(lock, pi), GetReal(lock) ));
//SetReal(servers, 'pi', pi);
//GetReal(servers, 'pi');


/*
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
*/