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

#include "platform.h"
#include "jlog.hpp"
#include "rtlbcd.hpp"

#include "nbcd.hpp"

#define _elements_in(a) (sizeof(a)/sizeof((a)[0]))

const char * hex = "0123456789ABCDEF";

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

// Usage: success &= check(statement, "error: foo bar %d", variable);
static bool check(bool condition, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static bool check(bool condition, const char *fmt, ...)
{
    if (!condition)
    {
        va_list args;
        va_start(args, fmt);
        VALOG(MCdebugInfo, unknownJob, fmt, args);
        va_end(args);
    }
    return condition;
}

// Usage: cppunit_assert(statement, "error: foo bar %d", variable));
static void cppunit_assert(bool condition, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void cppunit_assert(bool condition, const char *fmt, ...)
{
    if (!condition)
    {
        va_list args;
        va_start(args, fmt);
        VALOG(MCdebugInfo, unknownJob, fmt, args);
        va_end(args);
        CPPUNIT_ASSERT(!"Please refer to the errors above");
    }
}
// Do not use: cppunit_cppunit_assert(condition, "string")), as that will print the string twice

class NBcdTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE(NBcdTest);
        CPPUNIT_TEST(testBcdUninitialized);
        CPPUNIT_TEST(testBcdCString);
        CPPUNIT_TEST(testBcdRoundTruncate);
        CPPUNIT_TEST(testBcdDecimal);
        CPPUNIT_TEST(testBcdInt);
        CPPUNIT_TEST(testBcdMultiply);
        CPPUNIT_TEST(testBcdDivideModulus);
        CPPUNIT_TEST(testBcdCompare);
        // Failing tests (due to precision)
        CPPUNIT_TEST(testBcdRandom);
        CPPUNIT_TEST(testBcdPower);
        CPPUNIT_TEST(testBcdPrecision);
    CPPUNIT_TEST_SUITE_END();
protected:

    static void expandHex(const void * bytes, unsigned size, char * target)
    {
        byte * src = (byte *)bytes;
        while (size--)
        {
            *target++ = hex[*src>>4];
            *target++ = hex[*src&15];
            src++;
        }
        *target=0;
    }

    void testMultiply(const char * left, const char * right, const char * expected)
    {
        if (!right) right = left;
        char temp[80];
        Decimal a = left;
        Decimal b = right;
        a.multiply(b);
        a.getCString(sizeof(temp), temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: testMultiply/getCString: expected '%s', got '%s'", expected, temp);
        DecPushCString(left);
        DecPushCString(right);
        DecMul();
        DecPopCString(sizeof(temp),temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: testMultiply/DecMul: expected '%s', got '%s'", expected, temp);
    }

    void testDivide(const char * left, const char * right, const char * expected)
    {
        char temp[80];
        Decimal a = left;
        Decimal b = right;
        a.divide(b);
        a.getCString(sizeof(temp), temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: testDivide/getCString: expected '%s', got '%s'", expected, temp);
        DecPushCString(left);
        DecPushCString(right);
        DecDivide(DBZzero);
        DecPopCString(sizeof(temp),temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: testDivide/DecDivide: expected '%s', got '%s'", expected, temp);
    }

    void testCompare(const char * left, const char * right, int expected)
    {
        Decimal a = left;
        Decimal b = right;
        int temp = a.compare(b);
        cppunit_assert(temp == expected, "ERROR: testCompare/positive: expected '%d', got '%d'", expected, temp);
        temp = b.compare(a);
        cppunit_assert(temp == -expected, "ERROR: testCompare/negative: expected '%d', got '%d'", expected, temp);

        DecPushCString(left);
        DecPushCString(right);
        temp = DecDistinct();
        cppunit_assert(expected == temp, "ERROR: testCompare/DecDistinct: expected '%d', got '%d'", expected, temp);
    }

    void testModulus(const char * left, const char * right, const char * expected)
    {
        char temp[80];
        Decimal a = left;
        Decimal b = right;
        a.modulus(b);
        a.getCString(sizeof(temp), temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: testModulus: expected '%s', got '%s'", expected, temp);
    }

    void checkDecimal(const Decimal & value, const char * expected)
    {
        char temp[80];
        value.getCString(sizeof(temp), temp);
        const char * unknown = strchr(expected, 'x');
        if (unknown && temp[unknown-expected])
            temp[unknown-expected] = 'x';
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: checkDecimal/char: expected '%s', got '%s'", expected, temp);
    }

    void checkDecimal(const Decimal & value, unsigned __int64 expected)
    {
        unsigned __int64 temp = value.getUInt64();
        cppunit_assert(expected == temp, "ERROR: checkDecimal/uint64: expected '%" I64F "d', got '%" I64F "d'", expected, temp);
    }

    void checkDecimal(const Decimal & value, __int64 expected)
    {
        __int64 temp = value.getInt64();
        cppunit_assert(expected == temp, "ERROR: checkDecimal/int64: expected '%" I64F "d', got '%" I64F "d'", expected, temp);
    }

    void checkBuffer(const void * buffer, const char * expected)
    {
        char temp[40];
        expandHex(buffer, strlen(expected)/2, temp);
        cppunit_assert(strcmp(expected, temp) == 0, "ERROR: checkBuffer: expected '%s', got '%s'", expected, temp);
    }

    // ========================================================= UNIT TESTS BELOW
    void testBcdRandom()
    {
        for (int i = 0; i < 1000; i++)
        {
            // 14-digit numbers, multiplications can't pass 28 digits (32 max)
            unsigned __int64 val1 = ((__int64) fastRand() << 16) | fastRand();
            unsigned __int64 val2 = ((__int64) fastRand() << 16) | fastRand();
            unsigned __int64 val3 = ((__int64) fastRand() << 16) | fastRand();
            unsigned __int64 val4 = ((__int64) fastRand() << 16) | fastRand();

            for (int i = 0; i < 2; i++)
            {
                Decimal d1 = val1;
                Decimal d2 = val2;
                Decimal d3 = val3;
                Decimal d4 = val4;

                d1.multiply(d2);
                d3.multiply(d4);
                checkDecimal(d1, val1*val2);
                checkDecimal(d3, val3*val4);
                d2.set(d1);
                d1.subtract(d3);
                d2.add(d3);
                checkDecimal(d1, (__int64)(val1*val2-val3*val4));
                checkDecimal(d2, (val1*val2+val3*val4));
            }
        }
    }
    
    void testBcdUninitialized()
    {
        // Test uninitialised
        Decimal zero, one=1, two(2);
        checkDecimal(zero, 0ULL);
        checkDecimal(one, 1ULL);
        checkDecimal(two, 2ULL);
        zero.add(one);
        checkDecimal(zero, 1ULL);
        zero.multiply(two);
        checkDecimal(zero, 2ULL);
    }

    void testBcdCString()
    {
        Decimal a,b,c;
        a.setString(10,"1234.56789");   // 1234.56789
        b.setString(8,"  123456.88");   // 123456
        c.setString(6," 0.123 ");

        char temp[80];
        a.getCString(sizeof(temp), temp);
        check(strcmp("1234.56789", temp) == 0, "ERROR: testBcdCString/a: expected '1234.56789', got '%s'", temp);
        b.getCString(sizeof(temp), temp);
        check(strcmp("123456", temp) == 0, "ERROR: testBcdCString/b: expected '123456', got '%s'", temp);
        c.getCString(sizeof(temp), temp);
        check(strcmp("0.123", temp) == 0, "ERROR: testBcdCString/c: expected '0.123', got '%s'", temp);

        a.add(b);
        a.getCString(sizeof(temp), temp);
        check(strcmp("124690.56789", temp) == 0, "ERROR: testBcdCString/a+b: expected '124690.56789', got '%s'", temp);
        b.subtract(a);
        b.getCString(sizeof(temp), temp);
        check(strcmp("-1234.56789", temp) == 0, "ERROR: testBcdCString/-a: expected '-1234.56789', got '%s'", temp);
    }

    void testBcdRoundTruncate()
    {
        char temp[80];
        Decimal c = "9.53456";
        checkDecimal(c, "9.53456");
        c.round(4);
        checkDecimal(c,"9.5346");
        c.round(8);
        checkDecimal(c, "9.5346");
        c.round(2);
        checkDecimal(c, "9.53");
        c.round();
        checkDecimal(c, "10");

        c = 1234567.8901;
        checkDecimal(c, "1234567.8901");
        c.round(-3);
        checkDecimal(c, "1235000");

        c = "9.53456";
        c.truncate(4);
        checkDecimal(c, "9.5345");
        c.truncate(8);
        checkDecimal(c, "9.5345");
        c.truncate(2);
        checkDecimal(c, "9.53");
        c.truncate();
        checkDecimal(c, "9");

        Decimal x1 = 1;
        x1.round(-3);
        checkDecimal(x1, (__int64)0);
        Decimal x2 = 100;
        x2.round(-3);
        checkDecimal(x2, (__int64)0);
        Decimal x3 = 499;
        x3.round(-3);
        checkDecimal(x3, (__int64)0);
        Decimal x4 = 500;
        x4.round(-3);
        checkDecimal(x4, (__int64)1000);
        Decimal x5 = 1000;
        x5.round(-3);
        checkDecimal(x5, (__int64)1000);
        Decimal x6 = 1499;
        x6.round(-3);
        checkDecimal(x6, (__int64)1000);
        Decimal x7 = 1500;
        x7.round(-3);
        checkDecimal(x7, (__int64)2000);
        Decimal x8 = 10000;
        x8.round(-3);
        checkDecimal(x8, (__int64)10000);
        Decimal x9 = 10499;
        x9.round(-3);
        checkDecimal(x9, (__int64)10000);
        Decimal x10 = 10500;
        x10.round(-3);
        checkDecimal(x10, (__int64)11000);
        Decimal x11 = -10500;
        x11.round(-3);
        checkDecimal(x11, (__int64)-11000);

        c = 1234567.8901234567; // Expect rounding of the last digit
        c.getCString(sizeof(temp), temp);
        cppunit_assert(strcmp("1234567.890123457", temp) == 0, "ERROR: testBcdRoundTruncate/cstr: expected '1234567.890123457', got '%s'", temp);
        cppunit_assert(c.getReal() == 1234567.890123457, "ERROR: testBcdRoundTruncate/real: expected '1234567.890123457', got '%.8f'", c.getReal());
    }

    void testBcdDecimal()
    {
        Decimal a = "123.2345";
        unsigned decBufferSize=5;
        char decBuffer[7];
        char * decBufferPtr = decBuffer+1;
        decBuffer[0]=(char)0xCC;
        decBuffer[6]=(char)0xCC;

        a.getUDecimal(decBufferSize, 4, decBufferPtr);
        checkBuffer(decBuffer, "CC0001232345CC");
        a.getUDecimal(decBufferSize, 3, decBufferPtr);
        checkBuffer(decBuffer, "CC0000123234CC");
        a.getUDecimal(decBufferSize, 2, decBufferPtr);
        checkBuffer(decBuffer, "CC0000012323CC");
        a.getUDecimal(decBufferSize, 6, decBufferPtr);
        checkBuffer(decBuffer, "CC0123234500CC");
        a.getUDecimal(decBufferSize, 7, decBufferPtr);
        checkBuffer(decBuffer, "CC1232345000CC");
        a.getUDecimal(decBufferSize, 8, decBufferPtr);
        checkBuffer(decBuffer, "CC2323450000CC");

        a = "0.0001";
        a.getUDecimal(decBufferSize, 4, decBufferPtr);
        checkBuffer(decBuffer, "CC0000000001CC");
        a.getUDecimal(decBufferSize, 3, decBufferPtr);
        checkBuffer(decBuffer, "CC0000000000CC");

        a = "123.2345";
        a.getDecimal(decBufferSize, 4, decBufferPtr);
        checkBuffer(decBuffer, "CC001232345FCC");
        a.getDecimal(decBufferSize, 3, decBufferPtr);
        checkBuffer(decBuffer, "CC000123234FCC");
        a.getDecimal(decBufferSize, 2, decBufferPtr);
        checkBuffer(decBuffer, "CC000012323FCC");
        a.getDecimal(decBufferSize, 5, decBufferPtr);
        checkBuffer(decBuffer, "CC012323450FCC");
        a.getDecimal(decBufferSize, 6, decBufferPtr);
        checkBuffer(decBuffer, "CC123234500FCC");
        a.getDecimal(decBufferSize, 7, decBufferPtr);
        checkBuffer(decBuffer, "CC232345000FCC");
        a.getDecimal(decBufferSize, 5, decBufferPtr, 0xEB);
        checkBuffer(decBuffer, "CC012323450ECC");

        a = "0.0001";
        a.getDecimal(decBufferSize, 4, decBufferPtr);
        checkBuffer(decBuffer, "CC000000001FCC");
        a.getDecimal(decBufferSize, 3, decBufferPtr);
        checkBuffer(decBuffer, "CC000000000FCC");

        a = "-123.2345";
        a.getDecimal(decBufferSize, 4, decBufferPtr);
        checkBuffer(decBuffer, "CC001232345DCC");
        a.getDecimal(decBufferSize, 3, decBufferPtr, 0xFB);
        checkBuffer(decBuffer, "CC000123234BCC");

        memcpy(decBufferPtr, "\x00\x12\x34\x56\x78", 5);
        a.setUDecimal(5, 4, decBufferPtr);
        checkDecimal(a, "1234.5678");
        a.setUDecimal(5, 3, decBufferPtr);
        checkDecimal(a,"12345.678");
        a.setUDecimal(5, 0, decBufferPtr);
        checkDecimal(a, "12345678");
        a.setUDecimal(5, 9, decBufferPtr);
        checkDecimal(a, "0.012345678");

        memcpy(decBufferPtr, "\x00\x12\x34\x56\x7D", 5);
        a.setDecimal(5, 4, decBufferPtr);
        checkDecimal(a, "-123.4567");
        a.setDecimal(5, 3, decBufferPtr);
        checkDecimal(a, "-1234.567");
        a.setDecimal(5, 0, decBufferPtr);
        checkDecimal(a, "-1234567");
        a.setDecimal(5, 8, decBufferPtr);
        checkDecimal(a,"-0.01234567");

        memcpy(decBufferPtr, "\x00\x12\x34\x56\x7F", 5);
        a.setDecimal(5, 4, decBufferPtr);
        checkDecimal(a, "123.4567");
        a.setDecimal(5, 3, decBufferPtr);
        checkDecimal(a, "1234.567");
        a.setDecimal(5, 0, decBufferPtr);
        checkDecimal(a, "1234567");
        a.setDecimal(5, 8, decBufferPtr);
        checkDecimal(a, "0.01234567");
    }

    void testBcdInt()
    {
        Decimal a, b;
        for (unsigned i1 = 0; i1 <= 1000; i1++)
        {
            a = i1;
            cppunit_assert(a.getUInt() == i1, "ERROR: testBcdInt/getUInt: expected '%d', got '%d'", i1, a.getUInt());
        }
        for (unsigned i3 = 0; i3 <= 100; i3++)
        {
            a = i3;
            b = 10;
            a.multiply(b);
            cppunit_assert(a.getUInt() == i3*10, "ERROR: testBcdInt/getUInt*3: expected '%d', got '%d'", i3*10, a.getUInt());
        }

        for (unsigned i2 = 0; i2 <= 100; i2++)
        {
            Decimal x = i2;
            Decimal y = 100;
            y.multiply(x);
            cppunit_assert(100*i2 == (unsigned)y.getInt(), "ERROR: testBcdInt/getInt*100: expected '%d', got '%d'", 100*i2, y.getInt());
            x.multiply(x);
            cppunit_assert(i2*i2 == (unsigned)x.getInt(), "ERROR: testBcdInt/getInt*getInt: expected '%d', got '%d'", i2*i2, x.getInt());
        }
    }

    void testBcdMultiply()
    {
        testMultiply("-1","0","0");
        testMultiply("-1","2","-2");
        testMultiply("-1","-2","2");
        testMultiply("1","-2","-2");
        testMultiply("9","9","81");
        testMultiply("99","99","9801");
        testMultiply("999","999","998001");
        testMultiply("9999","9999","99980001");
        testMultiply("99.999999999",NULL,"9999.999999800000000001");
        testMultiply("9999.999999999",NULL,"99999999.999980000000000001");
        testMultiply("0.0000000000000001",NULL,"0.00000000000000000000000000000001");
        testMultiply("0.0000000000000009",NULL,"0.00000000000000000000000000000081");
        testMultiply("0.00000000000000001",NULL,"0");
        testMultiply("0.00000000000000009","0.0000000000000009","0.00000000000000000000000000000008");
        testMultiply("9999999999999999","10000000000000001","99999999999999999999999999999999");
        testMultiply("101","99009901","10000000001");
        testMultiply("0.000000000000000101","0.0000000000000000099009901","0");
        testMultiply("0.000000000000000101","0.000000000000000099009901","0.00000000000000000000000000000001");
        testMultiply("109", "9174311926605504587155963302.75229357798165137614678899082568", "999999999999999999999999999999.99999999999999999999999999999912");
        testMultiply("109", "9174311926605504587155963302.75229357798165137614678899082569", "1000000000000000000000000000000.00000000000000000000000000000021");
        testMultiply("9999999999.999999999999999999999999999999","9999999999.999999999999999999999999999999","99999999999999999999.99999999999999999998"); // actually 99999999999999999999.999999999999999999980000000000000000000000000000000000000001

        Decimal a = "9999999999999999";
        Decimal b = "10000000000000002";
        char temp[80];
        a.multiply(b);
        a.getCString(sizeof(temp), temp);
        cppunit_assert(strcmp("9999999999999998", temp) == 0, "ERROR: testBcdMultiply/overflow: expected '9999999999999998', got '%s'", temp);
    }

    void testBcdDivideModulus()
    {
        //Divide
        testDivide("1","1","1");
        testDivide("125","5","25");
        testDivide("99980001","9999","9999");
        testDivide("0.1234","10000000000000000000000000000000","0.00000000000000000000000000000001");
        testDivide("0.1234","20000000000000000000000000000000","0.00000000000000000000000000000001");
        testDivide("0.1234","30000000000000000000000000000000","0");
        testDivide("1","0.00000000000000000000000000000002", "50000000000000000000000000000000");
        testDivide("1","3", "0.33333333333333333333333333333333");
        testDivide("1000000000000000000000000000000","109", "9174311926605504587155963302.75229357798165137614678899082569");
        testModulus("1000000000000000000000000000000","109", "82");
        testModulus("10","5","0");
        testModulus("10","6","4");
        testModulus("10","-6","4");
        testModulus("-10","6","-4");
        testModulus("-10","-6","-4");
    }

    void testBcdCompare()
    {
        testCompare("1","1.0000",0);
        testCompare("-1","1.0000",-1);
        testCompare("1","-1.0000",+1);
        testCompare("-1","-1.0000",0);
        testCompare("1","2.0000",-1);
        testCompare("-1","2.0000",-1);
        testCompare("1","-2.0000",+1);
        testCompare("-1","-2.0000",+1);
        testCompare("100","2.0000",+1);
        testCompare("-100","2.0000",-1);
        testCompare("100","-2.0000",+1);
        testCompare("-100","-2.0000",-1);
        testCompare("0","1",-1);
        testCompare("0","-1",+1);
        testCompare("0","0",0);

        testCompare("1234","1230",+1);
        testCompare("1234.0001","1230.99",+1);
        testCompare("1234.999","1234.99",+1);
        testCompare("1234.989","1234.99",-1);
        testCompare("-1234","-1230",-1);
        testCompare("-1234.0001","-1230.99",-1);
        testCompare("-1234.999","-1234.99",-1);
        testCompare("-1234.989","-1234.99",+1);

    }

    void testBcdPower()
    {
        //MORE: Test power functions...
        const char * values[] = { "10000", "-1", "-10", "1.0001", "9.99" };
        Decimal one(1);
        for (unsigned idx = 0; idx < _elements_in(values); idx++)
        {
            Decimal value = values[idx];
            Decimal sofar1 = 1;
            Decimal sofar2 = 1;

            bool success=true;
            for (int power = 0; power < 10; power++)
            {
                Decimal powerValue1 = values[idx];
                Decimal powerValue2 = values[idx];
                powerValue1.power(power);
                powerValue2.power(-power);

                char temp1[80], temp2[80], temp3[80];
                if (sofar1.compare(powerValue1) != 0)
                {
                    Decimal diff = powerValue1;
                    diff.subtract(sofar1);
                    sofar1.getCString(sizeof(temp1), temp1);
                    powerValue1.getCString(sizeof(temp2), temp2);
                    diff.getCString(sizeof(temp3), temp3);
                    success &= check(false, "ERROR: %s^%d=%s (expected %s) diff %s", values[idx], power, temp2, temp1, temp3);
                }
                if (sofar2.compare(powerValue2) != 0)
                {
                    Decimal diff = powerValue2;
                    diff.subtract(sofar2);
                    sofar2.getCString(sizeof(temp1), temp1);
                    powerValue2.getCString(sizeof(temp2), temp2);
                    diff.getCString(sizeof(temp3), temp3);
                    success &= check(false, "ERROR: %s^%d=%s (expected %s) diff %s", values[idx], -power, temp2, temp1, temp3);
                }

                //internal consistency test, but liable to rounding errors....
                Decimal product(powerValue1);
                product.multiply(powerValue2);
                if (power && (product.compareNull() != 0) && (product.compare(one) != 0))
                {
                    char temp4[80];
                    char temp5[80];
                    Decimal diff = product;
                    diff.subtract(one);
                    one.getCString(sizeof(temp1), temp1);
                    product.getCString(sizeof(temp2), temp2);
                    diff.getCString(sizeof(temp3), temp3);
                    powerValue1.getCString(sizeof(temp4), temp4);
                    powerValue2.getCString(sizeof(temp5), temp5);
                    //Report rounding errors, but don't trigger a failure
                    check(false, "ERROR: %s^%d^-%d=%s (expected %s) diff %s [%s*%s]", values[idx], power, power, temp2, temp1, temp3, temp4, temp5);
                }

                sofar1.multiply(value);
                sofar2.divide(value);
            }
            cppunit_assert(success, "ERROR: testBcdPower: one or more errors detected above.");
        }
    }

    void testBcdPrecision()
    {
        //check rounding is done correctly to number of significant digits
        checkDecimal(9999999.12, "9999999.12");
        checkDecimal(-9999999.12, "-9999999.12");
        checkDecimal(9999999.12345678, "9999999.12345678");
        checkDecimal(-9999999.12345678, "-9999999.12345678");
        checkDecimal(9999999.123456789, "9999999.123456789");
        checkDecimal(-9999999.123456789, "-9999999.123456789");

        //MORE: The exact values are out of our control.
        //Real->decimal extracts 16 decimal digits, but only 15.9 are significant, so the last digit cannot be guaranteed.
        checkDecimal(91999991234567800.00, "919999912345678x0");
        checkDecimal(-91999991234567800.00, "-919999912345678x0");
        checkDecimal(91999991234567123.00, "919999912345671x0");

        // in vc++ these real constants seem to only have 14 significant digits
//      checkDecimal(0.99999991234567800, "0.999999912345678");
//      checkDecimal(0.99999991234567890, "0.999999912345679");
//      checkDecimal(0.099999991234567800, "0.0999999912345678");
//      checkDecimal(0.099999991234567890, "0.0999999912345679");
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( NBcdTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( NBcdTest, "NBcdTest" );

#endif
