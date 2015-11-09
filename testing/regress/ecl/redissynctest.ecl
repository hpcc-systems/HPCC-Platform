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

//nohthor

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

//Test Publish and Subscribe
//SUM(NOFOLD(s1 + s2), a) uses two threads - this test relies on this fact to work!
INTEGER N2 := 1000;
subDS := DATASET(N2, TRANSFORM({ integer a }, SELF.a := (INTEGER)myRedis.Subscribe('PubSubTest' + (STRING)COUNTER)));

INTEGER pub(STRING channel) := FUNCTION
        sl := Std.System.Debug.Sleep(10);
        value :=  myRedis.Publish(channel, '1');
     RETURN WHEN(value, sl, BEFORE);
END;
pubDS := DATASET(N2, TRANSFORM({ integer a }, SELF.a := pub('PubSubTest' + (STRING)COUNTER)));

INTEGER pub2(STRING channel) := FUNCTION
        sl := SEQUENTIAL(
            Std.System.Debug.Sleep(10),
            myRedis.Publish(channel, '3')//This pub is the one read by the sub.
            );
        value :=  myRedis.Publish(channel, '10000');//This pub isn't read by the sub, however the returned subscription count is present in the sum
     RETURN WHEN(value, sl, BEFORE);
END;
pubDS2 := DATASET(N2, TRANSFORM({ integer a }, SELF.a := pub2('PubSubTest' + (STRING)COUNTER)));

value := SUM(NOFOLD(subDS + pubDS2), a);
SEQUENTIAL(
    OUTPUT(SUM(NOFOLD(subDS + pubDS), a));//answer = N*2 = 2000
    OUTPUT( IF (value > N2*4, (STRING)value, 'OK'));//ideally result = N*3, less than this => not all subs had pubs (but this would cause a timeout).
    );
    // N*3 > result < N*4 => all subs received a pub, however, there were result-N*3 subs still open for the second pub. result > N*4 => gremlins.

myRedis.FlushDB();
myRedis2.FlushDB();
