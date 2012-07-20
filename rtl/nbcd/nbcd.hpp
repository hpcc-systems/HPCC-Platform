/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _NBCD_
#define _NBCD_

#ifdef _WIN32
 #ifdef NBCD_EXPORTS
  #define nbcd_decl __declspec(dllexport)
 #else
  #define nbcd_decl __declspec(dllimport)
 #endif
#else
 #define nbcd_decl
#endif

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
    Decimal() { setZero(); }
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
    byte digits[maxDigits];                       // stored little endian.
    byte msb;                                     // Most significant integer digit
    byte lsb;                                     // Least significant decimal digit
    byte negative;                                // byte to allow ^ operation
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

bool dec2Bool(size32_t bytes, const void * data);
bool udec2Bool(size32_t bytes, const void * data);
int decCompareDecimal(size32_t bytes, const void * _left, const void * _right);
int decCompareUDecimal(size32_t bytes, const void * _left, const void * _right);
bool decValid(bool isSigned, unsigned digits, const void * data);

#endif
