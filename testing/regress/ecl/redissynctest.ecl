/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

IMPORT sync FROM lib_redis;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
sync.FlushDB(server, /*database*/, password);

sync.SetBoolean('b', TRUE, server, /*database*/, /*expire*/, password);
sync.GetBoolean('b', server, /*database*/, password);

IMPORT redisSync FROM lib_redis;
myRedis := redisSync(server, password);

REAL pi := 3.14159265359;
myRedis.SetReal('pi', pi);
myRedis.GetReal('pi');

REAL pi2 := pi*pi;
myRedis.SetReal('pi', pi2, 1);
myRedis.GetReal('pi', 1);

INTEGER i := 123456789;
myRedis.SetInteger('i', i);
myRedis.GetInteger('i');

myRedis2 := redisSync('--SERVER=127.0.0.1:6380', 'youarefoobared');

myRedis2.SetReal('pi', pi2, 1);
myRedis2.GetReal('pi', 1);

myRedis2.SetInteger('i', i);
myRedis2.GetInteger('i');

UNSIGNED u := 7;
myRedis.SetUnsigned('u', u);
myRedis.GetUnsigned('u');

STRING str  := 'supercalifragilisticexpialidocious';
myRedis.SetString('str', str);
myRedis.GetString('str');

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
myRedis.setUnicode('uni', uni);
myRedis.getUnicode('uni');

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
myRedis.SetUtf8('utf8', utf);
myRedis.GetUtf8('utf8');

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
myRedis.SetData('data', mydata);
myRedis.GetData('data');

SEQUENTIAL(
    myRedis.Exists('utf8'),
    myRedis.Del('utf8'),
    myRedis.Exists('uft8')
    );

myRedis.Expire('str', 1); 
myRedis.Persist('str');

myRedis.GetInteger('pi');

NOFOLD(myRedis.DBSize());
NOFOLD(myRedis.DBSize(1));
NOFOLD(myRedis.DBSize(2));

SEQUENTIAL(
    myRedis.FlushDB(),
    NOFOLD(myRedis.Exists('str'))
    );
myRedis2.FlushDB();

//The follwoing tests the multithreaded caching of the redis connections
//SUM(NOFOLD(s1 + s2), a) uses two threads
myRedis.FlushDB();
INTEGER x := 2;
INTEGER N := 100;
myRedis.SetInteger('i', x);
s1 :=DATASET(N, TRANSFORM({ integer a }, SELF.a := NOFOLD(myRedis.GetInteger('i'))));
s2 :=DATASET(N, TRANSFORM({ integer a }, SELF.a := NOFOLD(myRedis.GetInteger('i'))/2));
SUM(NOFOLD(s1 + s2), a);//answer = (x+x/2)*N, in this case 3N.
myRedis.FlushDB();
