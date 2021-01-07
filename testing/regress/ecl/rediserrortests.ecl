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
//class=3rdpartyservice

//nothor

IMPORT * FROM lib_redis;
IMPORT Std;

// If some reason this port address should change, don't forget to do same 
// change in the relevant section (Result 3) in key/rediserrortest.xml file
STRING emptyPort := '6378';

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';

// The database is need to avoid crossover manipulation when multiple redis tests
// executed in parallel
INTEGER4 database := 0;
INTEGER4 invalidDatabase := 16;
redis.FlushDB(server, database, password);
myRedis := redisServer(server, password);

myFuncStr(STRING key) := FUNCTION
 value := myRedis.GetString(key, database);
 return value;
END;
myFuncUtf8(STRING key) := FUNCTION
 value := myRedis.GetUtf8(key, database);
 return value;
END;
myFuncUni(STRING key) := FUNCTION
 value := myRedis.GetUnicode(key, database);
 return value;
END;

getString(STRING key, STRING key2, myFuncStr func) := FUNCTION
    value := myRedis.GetOrLockString(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishString(key, func(key2), database), value);
END;
getUtf8(STRING key, STRING key2, myFuncUtf8 func) := FUNCTION
    value := myRedis.GetOrLockUtf8(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUtf8(key, func(key2), database), value);
END;
getUnicode(STRING key, STRING key2, myFuncUni func) := FUNCTION
    value := myRedis.GetOrLockUnicode(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUnicode(key, func(key2), database), value);
END;

//Test some exceptions
myRedis4 := RedisServer(server);
STRING noauth := 'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : NOAUTH Authentication required.';
STRING opNotPerm :=  'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : ERR operation not permitted';
ds1 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis4.GetString('authTest' + (STRING)COUNTER, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    OUTPUT(CATCH(ds1, ONFAIL(TRANSFORM({ STRING value }, SELF.value := IF(FAILMESSAGE = noauth OR FAILMESSAGE = opNotPerm, 'Auth Failed', 'Unexpected Error - ' + FAILMESSAGE)))));
    );

ds2 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis.GetString('authTest' + (STRING)COUNTER, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    OUTPUT(CATCH(ds2, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

myRedis5 := RedisServer('--SERVER=127.0.0.1:'+ emptyPort);
ds3 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis5.GetString('connectTest' + (STRING)COUNTER, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    OUTPUT(CATCH(ds3, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds4 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := redis.GetString('option' + (STRING)COUNTER, 'blahblahblah')));
SEQUENTIAL(
    OUTPUT(CATCH(ds4, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds5 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis.GetString('maxDB' + (STRING)COUNTER, invalidDatabase)));
SEQUENTIAL(
    OUTPUT(CATCH(ds5, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

//Test exception for checking expected channels
ds6 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetOrLockString('channelTest' + (string)COUNTER, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.SetString('channelTest1', 'redis_ecl_lock_blah_blah_blah', database);
    OUTPUT(CATCH(ds6, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds7 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetOrLockString('channelTest' + (string)(1+COUNTER), database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.SetString('channelTest2', 'redis_ecl_lock_channelTest2_0', database);
    OUTPUT(CATCH(ds7, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

//Test timeout
myRedisNoTO := redisServerWithoutTimeout(server, password);
dsTO := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedisNoTO.GetOrLockString('timeoutTest' + (string)COUNTER, database,,1000)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.GetOrLockString('timeoutTest1', database);
    OUTPUT(CATCH(dsTO, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

STRING pluginTO := 'Redis Plugin: ERROR - function timed out internally.';
STRING redisTO := 'Redis Plugin: ERROR - GetOrLock<type> \'timeoutTest2\' on database 0 for 127.0.0.1:6379 failed : Resource temporarily unavailable';
STRING authTO := 'Redis Plugin: ERROR - server authentication for 127.0.0.1:6379 failed : Resource temporarily unavailable';
STRING getSetTO := 'Redis Plugin: ERROR - SET %b %b NX PX 1000 \'timeoutTest2\' on database 0 for 127.0.0.1:6379 failed : Resource temporarily unavailable';
STRING subscribeTO := 'Redis Plugin: ERROR - SUBSCRIBE &apos;redis_ecl_lock_timeoutTest2_0&apos; on database 0 for 127.0.0.1:6379 failed : Resource temporarily unavailable';
dsTO2 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := redis.GetOrLockString('timeoutTest' + (string)(1+COUNTER), server, database, password, 100/*timeout ms*/)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.GetOrLockString('timeoutTest2', database);
    OUTPUT(CATCH(dsTO2, ONFAIL(TRANSFORM({ STRING value }, SELF.value := IF(FAILMESSAGE = pluginTO
                                                                         OR FAILMESSAGE = redisTO
                                                                         OR FAILMESSAGE = authTO
                                                                         OR FAILMESSAGE = getSetTO
                                                                         OR FAILMESSAGE = subscribeTO,
                                                                         'Timed Out', 'Unexpected Error - ' + FAILMESSAGE)))));
    );

STRING pluginIntExpected := 'Redis Plugin: ERROR - INCRBY \'testINCRBY1\' on database 0 for 127.0.0.1:6379 failed : ERR value is not an integer or out of range';
dsINCRBY := DATASET(NOFOLD(1), TRANSFORM({INTEGER value}, SELF.value := myRedis.INCRBY('testINCRBY' + (string)COUNTER, 11, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.setString('testINCRBY1', 'this is a string and not an integer', database),
    OUTPUT(CATCH(dsINCRBY, ONFAIL(TRANSFORM({ INTEGER value }, SELF.value := IF(FAILMESSAGE = pluginIntExpected, 1, 0) ))));
    );

STRING erange := 'Redis Plugin: ERROR - INCRBY \'testINCRBY11\' on database 0 for 127.0.0.1:6379 failed : ERR value is not an integer or out of range';
dsINCRBY2 := DATASET(NOFOLD(1), TRANSFORM({INTEGER value}, SELF.value := myRedis.INCRBY('testINCRBY1' + (string)COUNTER, -11, database)));
SEQUENTIAL(
    myRedis.FlushDB(database);
    myRedis.setUnsigned('testINCRBY11', -1, database),
    OUTPUT(CATCH(dsINCRBY2, ONFAIL(TRANSFORM({ INTEGER value }, SELF.value := IF(FAILMESSAGE = erange, 1, 0) ))));
    );

myRedis.FlushDB(database);
