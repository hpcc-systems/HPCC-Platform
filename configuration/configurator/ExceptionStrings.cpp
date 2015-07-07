#include "jstring.hpp"
#include "ExceptionStrings.hpp"

const int nDefaultCode = 99;

IException *MakeExceptionFromMap(int code, enum eExceptionCodes eCode, const char* pMsg)
{
    static StringBuffer strExceptionMessage;

    strExceptionMessage.setf("Exception: %s\nPossible Action(s): %s\n",  pExceptionStringArray[eCode-1], pExceptionStringActionArray[eCode-1]);

    if (pMsg != NULL)
    {
        strExceptionMessage.append("\nAdditonal Infomation: ").append(pMsg);
    }

    return MakeStringException(code, "%s", strExceptionMessage.str());
}

IException *MakeExceptionFromMap(enum eExceptionCodes eCode, const char* pMsg)
{
    return MakeExceptionFromMap(nDefaultCode, eCode, pMsg);
}
