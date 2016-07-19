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

#ifndef _NBCD_
#define _NBCD_

#ifdef NBCD_EXPORTS
 #define nbcd_decl DECL_EXPORT
#else
 #define nbcd_decl DECL_IMPORT
#endif

#define DECIMAL_OVERLOAD

template <byte length, byte precision> class decimal;

/*
 * Decimal implements Binary Coded Decimal (BCD) arithmetic.
 *
 * The internal representation is an array of digits, with size
 * equals maxDigits, divided in two sections:
 *  * 0~max/2: integer part
 *  * max/2+1~max: decimal part
 *
 * The decimal point is always in the middle and msb and lsb
 * point to the beginning of the integer part and the end
 * of the decimal part. Sign is a boolean flag, but represented
 * with standard C/D/F flags (+/-/unsigned) in BCD format.
 *
 * This class is notably used in the Decimal run-time library
 * (nbcds.cpp), providing decimal arithmetic to ECL programs.
 */
class nbcd_decl Decimal
{
public:
    constexpr Decimal() = default;
    Decimal(const Decimal & other);

    Decimal & abs();
    Decimal & add(const Decimal & other);
    int compareNull() const;
    int compare(const Decimal & other) const;
    Decimal & divide(const Decimal & other);
    Decimal & modulus(const Decimal & other);
    Decimal & multiply(const Decimal & other);
    Decimal & negate();
    Decimal & power(int value);
    Decimal & power(unsigned value);
    Decimal & round(int places=0);      // -ve means left of decimal point e.g., -3 = to nearest 1000.
    Decimal & roundup(int places=0);        // -ve means left of decimal point e.g., -3 = to nearest 1000.
    Decimal & setPrecision(byte numDigits, byte precision);
    Decimal & subtract(const Decimal & other);
    Decimal & truncate(int places=0);

    size32_t getStringLength() const;
    void getCString(size32_t length, char * buffer) const;
    char * getCString() const;
    void getDecimal(byte length, byte precision, void * buffer, byte signs=0xFD) const;
    __int64 getInt64() const;
    int getInt() const;
    double getReal() const;
    void getString(size32_t length, char * buffer) const;
    void getStringX(size32_t & length, char * & buffer) const;
    void getUDecimal(byte length, byte precision, void * buffer) const;
    unsigned __int64 getUInt64() const;
    unsigned int getUInt() const;

    void getClipPrecision(unsigned & digits, unsigned & precision);
    void getPrecision(unsigned & digits, unsigned & precison);

    // MORE: We could support NaNs for decimals at a later date by adding a member to this class.
    bool isZero() const;
    bool isValid() const { return true; }

    void set(const Decimal & value);
    void setCString(const char * buffer);
    void setDecimal(byte length, byte precision, const void * buffer);
    void setInt64(__int64 value);
    void setInt(int value);
    void setReal(double value);
    void setString(size32_t length, const char * buffer);
    void setUInt64(unsigned __int64 value);
    void setUInt(unsigned int value);
    void setUDecimal(byte length, byte precision, const void * buffer);
    void setZero();

#ifdef DECIMAL_OVERLOAD
    template <byte length, byte precision> 
    Decimal(const decimal<length, precision> & x)   { setDecimal(length, precision, &x); }
    inline Decimal(int value)                               { setInt(value); }
    inline Decimal(unsigned value)                          { setUInt(value); }
    inline Decimal(__int64 value)                           { setInt64(value); }
    inline Decimal(unsigned __int64 value)                  { setUInt64(value); }
    inline Decimal(double value)                            { setReal(value); }
    inline Decimal(const char * value)                      { setCString(value); }

    inline Decimal & operator = (int value)                 { setInt(value); return *this; }
    inline Decimal & operator = (unsigned value)            { setUInt(value); return *this; }
    inline Decimal & operator = (__int64 value)             { setInt64(value); return *this; }
    inline Decimal & operator = (unsigned __int64 value)    { setUInt64(value); return *this; }
    inline Decimal & operator = (double value)              { setReal(value); return *this; }
    inline Decimal & operator = (const char * value)        { setCString(value); return *this; }
#endif

protected:
    Decimal & addDigits(const Decimal & other);
    Decimal & subtractDigits(const Decimal & other);
    void clip(int & newLsb, int & newMsb) const;
    void clip(int & newLsb, int & newMsb, unsigned minLsb, unsigned maxMsb) const;
    void doGetDecimal(byte length, byte maxDigits, byte precision, void * buffer) const;
    void doPower(unsigned value);

    unsigned doGetString(char * target) const;
    void extendRange(byte oLsb, byte oMsb);
    void extendRange(const Decimal & other)     { extendRange(other.lsb, other.msb); }
    void overflow();

private:
    Decimal & incLSD();

protected:
    enum { 
        maxDigits=MAX_DECIMAL_DIGITS,             // Total buffer size (integer+decimal)
        maxPrecision=MAX_DECIMAL_PRECISION,       // Size of decimal part
        maxIntegerDigits=MAX_DECIMAL_LEADING,     // Size of integer part
        lastDigit = maxDigits-1,                  // Last decimal digit
        zeroDigit = (maxDigits-maxIntegerDigits), // Unity digit (decimal point)
    };
    byte digits[maxDigits] = { 0 } ;              // stored little endian.
    byte msb = zeroDigit;                         // Most significant integer digit
    byte lsb = zeroDigit;                         // Least significant decimal digit
    byte negative = false;                        // byte to allow ^ operation
};


template <byte length, byte precision>
class decimal
{
public:
    decimal()                               { memset(buffer, 0, length); }
    decimal(const Decimal & other)      { other.getDecimal(length, precision, buffer); }

protected:
    byte buffer[length];
};

template <byte length, byte precision>
class udecimal
{
public:
    udecimal()                              { memset(buffer, 0, length); }
    udecimal(const Decimal & other)     { other.getUDecimal(length, precision, buffer); }

protected:
    byte buffer[length];
};

#ifdef DECIMAL_OVERLOAD
inline Decimal operator + (const Decimal & left, const Decimal & right) { return Decimal(left).add(right); }
inline Decimal operator - (const Decimal & left, const Decimal & right) { return Decimal(left).subtract(right); }
inline Decimal operator * (const Decimal & left, const Decimal & right) { return Decimal(left).multiply(right); }
inline Decimal operator / (const Decimal & left, const Decimal & right) { return Decimal(left).divide(right); }
inline Decimal operator % (const Decimal & left, const Decimal & right) { return Decimal(left).modulus(right); }
inline bool operator == (const Decimal & left, const Decimal & right) { return left.compare(right) == 0; }
inline bool operator != (const Decimal & left, const Decimal & right) { return left.compare(right) != 0; }
inline bool operator >= (const Decimal & left, const Decimal & right) { return left.compare(right) >= 0; }
inline bool operator <= (const Decimal & left, const Decimal & right) { return left.compare(right) <= 0; }
inline bool operator > (const Decimal & left, const Decimal & right) { return left.compare(right) > 0; }
inline bool operator < (const Decimal & left, const Decimal & right) { return left.compare(right) < 0; }
#endif

//Various utility helper functions:
nbcd_decl bool dec2Bool(size32_t bytes, const void * data);
nbcd_decl bool udec2Bool(size32_t bytes, const void * data);
nbcd_decl int decCompareDecimal(size32_t bytes, const void * _left, const void * _right);
nbcd_decl int decCompareUDecimal(size32_t bytes, const void * _left, const void * _right);
nbcd_decl bool decValid(bool isSigned, unsigned digits, const void * data);

#endif
