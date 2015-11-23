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

IMPORT * FROM lib_redis;
IMPORT Std;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
redis.FlushDB(server, /*database*/, password);
myRedis := redisServer(server, password);

myFuncStr(STRING key) := FUNCTION
 value := myRedis.GetString(key);
 return value;
END;
myFuncUtf8(STRING key) := FUNCTION
 value := myRedis.GetUtf8(key);
 return value;
END;
myFuncUni(STRING key) := FUNCTION
 value := myRedis.GetUnicode(key);
 return value;
END;

getString(STRING key, STRING key2, myFuncStr func) := FUNCTION
    value := myRedis.GetOrLockString(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishString(key, func(key2)), value);
END;
getUtf8(STRING key, STRING key2, myFuncUtf8 func) := FUNCTION
    value := myRedis.GetOrLockUtf8(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUtf8(key, func(key2)), value);
END;
getUnicode(STRING key, STRING key2, myFuncUni func) := FUNCTION
    value := myRedis.GetOrLockUnicode(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUnicode(key, func(key2)), value);
END;

//Test some exceptions
myRedis4 := RedisServer(server);
STRING noauth := 'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : NOAUTH Authentication required.';
STRING opNotPerm :=  'Redis Plugin: ERROR - authentication for 127.0.0.1:6379 failed : ERR operation not permitted';
ds1 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis4.GetString('authTest' + (STRING)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds1, ONFAIL(TRANSFORM({ STRING value }, SELF.value := IF(FAILMESSAGE = noauth OR FAILMESSAGE = opNotPerm, 'Auth Failed', 'Unexpected Error - ' + FAILMESSAGE)))));
    );

ds2 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis.GetString('authTest' + (STRING)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds2, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

myRedis5 := RedisServer('--SERVER=127.0.0.1:9999');
ds3 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis5.GetString('connectTest' + (STRING)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    OUTPUT(CATCH(ds3, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds4 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := redis.GetString('option' + (STRING)COUNTER, 'blahblahblah')));
SEQUENTIAL(
    OUTPUT(CATCH(ds4, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds5 := DATASET(NOFOLD(1), TRANSFORM({STRING value}, SELF.value := myRedis.GetString('maxDB' + (STRING)COUNTER, 16)));
SEQUENTIAL(
    OUTPUT(CATCH(ds5, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

//Test exception for checking expected channels
ds6 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetOrLockString('channelTest' + (string)COUNTER)));
SEQUENTIAL(
    myRedis.FlushDB();
    myRedis.SetString('channelTest1', 'redis_ecl_lock_blah_blah_blah');
    OUTPUT(CATCH(ds6, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

ds7 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedis.GetOrLockString('channelTest' + (string)(1+COUNTER))));
SEQUENTIAL(
    myRedis.FlushDB();
    myRedis.SetString('channelTest2', 'redis_ecl_lock_channelTest2_0');
    OUTPUT(CATCH(ds7, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

//Test timeout
myRedisNoTO := redisServerWithoutTimeout(server, password);
dsTO := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := myRedisNoTO.GetOrLockString('timeoutTest' + (string)COUNTER,,,1000)));
SEQUENTIAL(
    myRedis.FlushDB();
    myRedis.GetOrLockString('timeoutTest1');
    OUTPUT(CATCH(dsTO, ONFAIL(TRANSFORM({ STRING value }, SELF.value := FAILMESSAGE))));
    );

STRING pluginTO := 'Redis Plugin: ERROR - function timed out internally.';
STRING redisTO := 'Redis Plugin: ERROR - GetOrLock<type> \'timeoutTest2\' on database 0 for 127.0.0.1:6379 failed : Resource temporarily unavailable';
STRING authTO := 'Redis Plugin: ERROR - server authentication for 127.0.0.1:6379 failed : Resource temporarily unavailable';
STRING getSetTO := 'Redis Plugin: ERROR - SET %b %b NX PX 1000 \'timeoutTest2\' on database 0 for 127.0.0.1:6379 failed : Resource temporarily unavailable';
dsTO2 := DATASET(NOFOLD(1), TRANSFORM({string value}, SELF.value := redis.GetOrLockString('timeoutTest' + (string)(1+COUNTER), server, /*database*/, password, 1/*ms*/)));
SEQUENTIAL(
    myRedis.FlushDB();
    myRedis.GetOrLockString('timeoutTest2');
    OUTPUT(CATCH(dsTO2, ONFAIL(TRANSFORM({ STRING value }, SELF.value := IF(FAILMESSAGE = pluginTO
                                                                         OR FAILMESSAGE = redisTO
                                                                         OR FAILMESSAGE = authTO
                                                                         OR FAILMESSAGE = getSetTO,
                                                                         'Timed Out', 'Unexpected Error - ' + FAILMESSAGE)))));
    );

myRedis.FlushDB();
