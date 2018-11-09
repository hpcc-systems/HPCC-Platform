//  cstrings.h  support for C++ cString class
//
#ifndef _CSTRING_H
#define _CSTRING_H

#include <string.h>
#include <ctype.h>

#define CSINITIALSIZE 8         // size of statically allocated string buffer

class cString
{
private:
    int     iBufLen;        // current buffer length
    char    buffer[CSINITIALSIZE];

protected:
    void Set(const char *Str) {
        if (!Str) {
            // treat null address as a null string
            Len = 0;
            *Ptr = '\0';
            return;
        }
        Len = strlen( Str );
        if (Len >= iBufLen && Len + 1 > CSINITIALSIZE) {
            if (iBufLen != CSINITIALSIZE) {
                delete [] Ptr;
            }
            iBufLen = Len + 1;
            Ptr = new char[iBufLen];
        }
        memcpy( Ptr, Str, Len+1 );
    }

public:
    int     Len;            // current string length
    char    *Ptr;           // current string buffer
    cString() {
        iBufLen = CSINITIALSIZE;
        Len = 0;
        Ptr = buffer;
        *Ptr = '\0';
    }

    cString(const char *Str) {
        Len = strlen( Str );
        iBufLen = Len + 1;
        if (iBufLen <= CSINITIALSIZE) {
            iBufLen = CSINITIALSIZE;
            Ptr = buffer;
        } else {
            Ptr = new char[ iBufLen ];
        }
        memcpy( Ptr, Str, Len+1 );
    }

    cString(const cString &cStr) {
        Len = cStr.Len;
        iBufLen = Len + 1;
        if (iBufLen <= CSINITIALSIZE) {
            iBufLen = CSINITIALSIZE;
            Ptr = buffer;
        } else {
            Ptr = new char[ iBufLen ];
        }
        memcpy( Ptr, cStr.Ptr, Len+1 );
    }

    inline ~cString() {
        if (iBufLen != CSINITIALSIZE) {
            delete [] Ptr;
        }
    }

    void Set(const char *Str, int len) {
        Len = len;
        if (Len >= iBufLen && Len + 1 > CSINITIALSIZE) {
            if (iBufLen != CSINITIALSIZE) {
                delete [] Ptr;
            }
            iBufLen = Len + 1;
            Ptr = new char[iBufLen];
        }
        memcpy( Ptr, Str, Len );
        Ptr[Len] = 0;
    }

    void SetLength(int len) {
        Len = len;
        if (Len >= iBufLen && Len + 1 > CSINITIALSIZE) {
            if (iBufLen != CSINITIALSIZE) {
                delete [] Ptr;
            }
            iBufLen = Len + 1;
            Ptr = new char[iBufLen];
        }
    }


    void Trim() {
        if (Len) {
            char    *sp;
            sp = Ptr + Len - 1;
            while (Len) {
                if (*sp == ' ') {
                    Len--;
                    sp--;
                } else {
                    break;
                }
            }
            *(sp+1) = 0;
        }
    }

    void Upper() {
        char *sp = Ptr;
        char *ep = Ptr + Len;

        while (sp < ep) {
            *sp = toupper_char(*sp);
            sp++;
        }
    }

    void Lower() {
        char *sp = Ptr;
        char *ep = Ptr + Len;

        while (sp < ep) {
            *sp = tolower(*sp);
            sp++;
        }
    }

    // Concatenate a string to the existing string
    void Cat(const char *string, int tlen) {
        if (Len + tlen >= iBufLen && Len + tlen + 1 >= CSINITIALSIZE) {
            char *tPtr = new char[Len + tlen + 1];
            memcpy( tPtr, Ptr, Len );
            if(iBufLen != CSINITIALSIZE)
                delete [] Ptr;
            iBufLen = Len + tlen + 1;
            Ptr = tPtr;
            Ptr[Len] = 0;
        }
        memcpy( Ptr + Len, string, tlen );
        Len += tlen;
        Ptr[Len] = 0;
    }

    inline void Cat(cString &string) {
        Cat(string.Ptr, string.Len);
    }

    inline void Cat( const char *string ) {
        Cat( string, strlen(string));
    }

    inline cString& operator =(const char *Str) {
        Set(Str);
        return(*this);
    }

    inline cString& operator=(cString &cStr) {
        Set(cStr.Ptr,cStr.Len);
        return *this;
    }

    cString& operator+=(cString &cStr) {
        if (Len + cStr.Len >= iBufLen && Len + cStr.Len + 1 > CSINITIALSIZE) {
            char *tPtr = new char[iBufLen + cStr.Len + 1];
            memcpy( tPtr, Ptr, Len );
            if(iBufLen != CSINITIALSIZE)
                delete [] Ptr;
            iBufLen = Len + cStr.Len + 1;
            Ptr = tPtr;
        }
        memcpy(Ptr+Len,cStr.Ptr,cStr.Len+1);
        Len += cStr.Len;
        return *this;
    }

    char *operator+=(const char *Str) {
        int slen = strlen(Str);
        if (Len + slen >= iBufLen && Len + slen + 1 > CSINITIALSIZE) {
            char *tPtr = new char[iBufLen + slen + 1];
            memcpy( tPtr, Ptr, Len );
            if(iBufLen != CSINITIALSIZE)
                delete [] Ptr;
            iBufLen = Len + slen + 1;
            Ptr = tPtr;
        }
        memcpy(Ptr+Len, Str, slen+1);
        Len += slen;;
        return Ptr;
    }

    cString& operator+=(const char ch)  {
        if (Len + 1 < iBufLen) {
            Ptr[Len++] = ch;
            Ptr[Len] = '\0';
        } else {
            Cat(&ch, 1);
        }
        return(*this);
    }

    inline operator char*() {
        return(Ptr);
    }
};

#endif  //_CSTRING_H
