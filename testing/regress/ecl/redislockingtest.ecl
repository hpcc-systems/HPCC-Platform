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

IMPORT redisServer FROM lib_redis;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
myRedis := redisServer(server, password);


myFunc(STRING key) := FUNCTION
 //out := output('uh oh!');
 value := myRedis.getString(key);
 //return WHEN(value, out);
 return value;
END;
key := 'Einstein';
key2 := 'Einnie sit';

CSleep(INTEGER duration) := BEGINC++
    sleep(duration);
ENDC++;

SEQUENTIAL(
    myRedis.FlushDB(),
    //myRedis.setString('redis-cli monitor trace', '************************************************');
    myRedis.setString(key2, 'Good boy Einnie!'),

    IF (myRedis.locking.Exists(key),
        myRedis.locking.GetString(key),
        myRedis.locking.SetString(key, myFunc(key2))
        ),
    myRedis.setString(key, 'This way kido'),
    IF (myRedis.locking.Exists(key),
        myRedis.locking.GetString(key),
        myRedis.locking.SetString(key, myFunc(key2))
        ),
    myRedis.flushdb()
    );

REAL getPi := 3.14159265359;
getMyReal(STRING key) := FUNCTION
  return myRedis.getReal(key);
END;

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.setReal('getPi', getPi),

    IF (myRedis.locking.Exists('pi'),
        myRedis.locking.GetReal('pi'),
        myRedis.locking.SetReal('pi', getMyReal('getPi'))
        ),
    IF (myRedis.locking.Exists('pi'),
        myRedis.locking.GetReal('pi'),
        myRedis.locking.SetReal('pi', getMyReal('getPi'))
        ),
    myRedis.flushdb()
    );

//Test unlock
SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.locking.exists('testlock'),/*by default lock expires after 1s*/
    myRedis.exists('testlock'),
    CSleep(2),
    myRedis.exists('testlock'),

    myRedis.setString('testlock', 'redis_ecl_lock_blah_blah_blah'),
    myRedis.exists('testlock'),
    myRedis.locking.unlock('testlock'),
    myRedis.exists('testlock'),
    myRedis.FlushDB(),
    );


myRedis.FlushDB();
