/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

//class=embedded
//class=3rdparty

//nothor

IMPORT redis FROM lib_redis;
IMPORT Std;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
redis.FlushDB(server, /*database*/, password);

SEQUENTIAL(
    redis.SetBoolean('b', TRUE, server, /*database*/, /*expire*/, password);
    redis.GetBoolean('b', server, /*database*/, password);
    );

IMPORT redisServer FROM lib_redis;
myRedis := redisServer(server, password);

REAL pi := 3.14159265359;
SEQUENTIAL(
    myRedis.SetReal('pi', pi);
    myRedis.GetReal('pi');
    myRedis.GetInteger('pi');
    );

REAL pi2 := pi*pi;
SEQUENTIAL(
    myRedis.SetReal('pi', pi2, 1);
    myRedis.GetReal('pi', 1);
    );

INTEGER i := 123456789;
SEQUENTIAL(
    myRedis.SetInteger('i', i);
    myRedis.GetInteger('i');
    );

myRedis2 := redisServer('--SERVER=127.0.0.1:6380', 'youarefoobared');
SEQUENTIAL(
    myRedis2.SetReal('pi', pi2, 1);
    myRedis2.GetReal('pi', 1);
    );

SEQUENTIAL(
    myRedis2.SetInteger('i', i);
    myRedis2.GetInteger('i');
    );

UNSIGNED u := 7;
SEQUENTIAL(
    myRedis.SetUnsigned('u', u);
    myRedis.GetUnsigned('u');
    );

myRedis3 := RedisServer('--SERVER=127.0.0.1:6381', password);
SEQUENTIAL(
    myRedis3.SetUnsigned('u3', u);
    myRedis3.GetUnsigned('u3');
    );

STRING str  := 'supercalifragilisticexpialidocious';
SEQUENTIAL(
    myRedis.SetString('str', str);
    myRedis.GetString('str');
    myRedis.FlushDB();
    myRedis.Exists('str');
    );

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SEQUENTIAL(
    myRedis.setUnicode('uni', uni);
    myRedis.getUnicode('uni');
    );

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SEQUENTIAL(
    myRedis.SetUtf8('utf8', utf);
    myRedis.GetUtf8('utf8');
    myRedis.Exists('utf8');
    myRedis.Delete('utf8');
    myRedis.Exists('uft8');
    );

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
SEQUENTIAL(
    myRedis.SetData('data', mydata);
    myRedis.GetData('data');
    );

SEQUENTIAL(
    myRedis.SetString('str2int', 'abcdefgh');//Heterogeneous calls will only result in an exception when the retrieved value does not fit into memory of the requested type.
    myRedis.GetInteger('str2int');
    );

sleep(INTEGER duration) := Std.System.Debug.Sleep(duration * 1000);

SEQUENTIAL(
    myRedis.Exists('str2'),
    myRedis.Expire('str2', , 900),/*\ms*/
    sleep(2),
    myRedis.Exists('str2'),

    myRedis.SetString('str3', str),
    myRedis.Exists('str3'),
    myRedis.Expire('str3', , 900),/*\ms*/
    myRedis.Persist('str3'),
    sleep(2),
    myRedis.Exists('str3')
    );

SEQUENTIAL(
    myRedis.SetString('Einnie', 'Woof', 0, 1000),
    myRedis.Exists('Einnie');
    sleep(2);
    myRedis.Exists('Einnie');

    myRedis.SetString('Einnie', 'Woof', 0),
    myRedis.SetString('Einnie', 'Grrrr', 1),
    myRedis.GetString('Einnie', 0),
    myRedis.GetString('Einnie', 1),
    myRedis.SetString('Einnie', 'Woof-Woof'),
    myRedis.GetString('Einnie'),
    );

myRedis.DBSize();
myRedis.DBSize(1);
myRedis.DBSize(2);

//The follwoing tests the multithreaded caching of the redis connections
//SUM(NOFOLD(s1 + s2), a) uses two threads
INTEGER x := 2;
INTEGER N := 100;
myRedis.FlushDB();
s1 := DATASET(N, TRANSFORM({ integer a }, SELF.a := myRedis.GetInteger('transformTest' +  'x'[1..NOFOLD(0)*COUNTER]   )));
s2 := DATASET(N, TRANSFORM({ integer a }, SELF.a := myRedis.GetInteger('transformTest' +  'x'[1..NOFOLD(0)*COUNTER]   )/2));
//'x'[1..NOFOLD(0)*COUNTER] prevents the compiler from obeying the 'once' keyword associated with the GetString service definition and
//therefore calls GetString('transformTest') 2N times as could be intended.  Without this, it is only called once/thread (in this case twice).
//In either case the resultant aggregrate below will return 1.5xN (1.5*2*100 = 300).
SEQUENTIAL(
    myRedis.SetInteger('transformTest', x),
    OUTPUT(SUM(NOFOLD(s1 + s2), a))//answer = (x+x/2)*N, in this case 300.
    );

//Test some exceptions
myRedis4 := RedisServer(server);
STRING noauth := 'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : NOAUTH Authentication required.';
STRING opNotPerm :=  'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : ERR operation not permitted';
ds1 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis4.GetString('authTest' + (string)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds1, ONFAIL(TRANSFORM({ STRING value }, SELF.value := IF(FAILMESSAGE = noauth OR FAILMESSAGE = opNotPerm, 'Auth Failed', 'Unexpected Error - ' + FAILMESSAGE)))));
    );

ds2 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetString('authTest' + (string)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds2, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

myRedis5 := RedisServer('--SERVER=127.0.0.1:9999');
ds3 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis5.GetString('connectTest' + (string)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds3, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds4 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := redis.GetString('option' + (string)COUNTER, 'blahblahblah')));
SEQUENTIAL(
    OUTPUT(CATCH(ds4, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds5 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetString('maxDB' + (string)COUNTER, 16)));
SEQUENTIAL(
    OUTPUT(CATCH(ds5, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

myRedis.FlushDB();
myRedis2.FlushDB();
