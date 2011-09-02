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

class nbcd_decl TempDecimal
{
public:
    TempDecimal() {} // does no initialization...
    TempDecimal(const TempDecimal & other);

    TempDecimal & abs();
    TempDecimal & add(const TempDecimal & other);
    int compareNull() const;
    int compare(const TempDecimal & other) const;
    TempDecimal & divide(const TempDecimal & other);
    TempDecimal & modulus(const TempDecimal & other);
    TempDecimal & multiply(const TempDecimal & other);
    TempDecimal & negate();
    TempDecimal & power(int value);
    TempDecimal & power(unsigned value);
    TempDecimal & round(int places=0);      // -ve means left of decimal point e.g., -3 = to nearest 1000.
    TempDecimal & roundup(int places=0);        // -ve means left of decimal point e.g., -3 = to nearest 1000.
    TempDecimal & setPrecision(byte numDigits, byte precision);
    TempDecimal & subtract(const TempDecimal & other);
    TempDecimal & truncate(int places=0);

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

    void set(const TempDecimal & value);
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
    TempDecimal(const decimal<length, precision> & x)   { setDecimal(length, precision, &x); }
    inline TempDecimal(int value)                               { setInt(value); }
    inline TempDecimal(unsigned value)                          { setUInt(value); }
    inline TempDecimal(__int64 value)                           { setInt64(value); }
    inline TempDecimal(unsigned __int64 value)                  { setUInt64(value); }
    inline TempDecimal(double value)                            { setReal(value); }
    inline TempDecimal(const char * value)                      { setCString(value); }

    inline TempDecimal & operator = (int value)                 { setInt(value); return *this; }
    inline TempDecimal & operator = (unsigned value)            { setUInt(value); return *this; }
    inline TempDecimal & operator = (__int64 value)             { setInt64(value); return *this; }
    inline TempDecimal & operator = (unsigned __int64 value)    { setUInt64(value); return *this; }
    inline TempDecimal & operator = (double value)              { setReal(value); return *this; }
    inline TempDecimal & operator = (const char * value)        { setCString(value); return *this; }
#endif

protected:
    TempDecimal & addDigits(const TempDecimal & other);
    TempDecimal & subtractDigits(const TempDecimal & other);
    void clip(int & newLsb, int & newMsb) const;
    void clip(int & newLsb, int & newMsb, unsigned minLsb, unsigned maxMsb) const;
    void doGetDecimal(byte length, byte maxDigits, byte precision, void * buffer) const;
    void doPower(unsigned value);

    unsigned doGetString(char * target) const;
    void extendRange(byte oLsb, byte oMsb);
    void extendRange(const TempDecimal & other)     { extendRange(other.lsb, other.msb); }
    void overflow();

private:
    TempDecimal & incLSD();

protected:
    enum { 
        maxDigits=MAX_DECIMAL_DIGITS,
        maxPrecision=MAX_DECIMAL_PRECISION,
        maxIntegerDigits=MAX_DECIMAL_LEADING,
        lastDigit = maxDigits-1, 
        zeroDigit = (maxDigits-maxIntegerDigits), 
    };
    byte digits[maxDigits];                 // stored little endian.
    byte msb;
    byte lsb;
    byte negative;                          // byte to allow ^ operation
};


template <byte length, byte precision>
class decimal
{
public:
    decimal()                               { memset(buffer, 0, length); }
    decimal(const TempDecimal & other)      { other.getDecimal(length, precision, buffer); }

protected:
    byte buffer[length];
};

template <byte length, byte precision>
class udecimal
{
public:
    udecimal()                              { memset(buffer, 0, length); }
    udecimal(const TempDecimal & other)     { other.getUDecimal(length, precision, buffer); }

protected:
    byte buffer[length];
};

#ifdef DECIMAL_OVERLOAD
inline TempDecimal operator + (const TempDecimal & left, const TempDecimal & right) { return TempDecimal(left).add(right); }
inline TempDecimal operator - (const TempDecimal & left, const TempDecimal & right) { return TempDecimal(left).subtract(right); }
inline TempDecimal operator * (const TempDecimal & left, const TempDecimal & right) { return TempDecimal(left).multiply(right); }
inline TempDecimal operator / (const TempDecimal & left, const TempDecimal & right) { return TempDecimal(left).divide(right); }
inline TempDecimal operator % (const TempDecimal & left, const TempDecimal & right) { return TempDecimal(left).modulus(right); }
inline bool operator == (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) == 0; }
inline bool operator != (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) != 0; }
inline bool operator >= (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) >= 0; }
inline bool operator <= (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) <= 0; }
inline bool operator > (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) > 0; }
inline bool operator < (const TempDecimal & left, const TempDecimal & right) { return left.compare(right) < 0; }
#endif

bool dec2Bool(size32_t bytes, const void * data);
bool udec2Bool(size32_t bytes, const void * data);
int decCompareDecimal(size32_t bytes, const void * _left, const void * _right);
int decCompareUDecimal(size32_t bytes, const void * _left, const void * _right);
bool decValid(bool isSigned, unsigned digits, const void * data);

#endif
