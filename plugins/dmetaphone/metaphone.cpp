#include "platform.h"
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include "metaphone.h"

////////////////////////////////////////////////////////////////////////////////
// Double Metaphone (c) 1998, 1999 by Lawrence Philips
//
// Slightly modified by Kevin Atkinson to fix several bugs and
// to allow it to give back more than 4 characters.
//
//  13-Dec-00   mtw Modified to return a number (e.g. 77th returns 77)
//
// Placed in the public domain by Lawrence Philips
//
////////////////////////////////////////////////////////////////////////////////
#include "metaphone.h"
#include <ctype.h>

#define AND &&
#define OR ||

namespace nsDmetaphone {

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
MString::MString()
{
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
MString::MString(const char* in) : cString(in)
{
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
MString::MString(const cString& in) : cString(in)
{
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
bool MString::SlavoGermanic()
{
    return (Find('W') OR Find('K') OR Find("CZ") OR Find("WITZ"));
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
inline void MString::MetaphAdd(const char* main)
{
    primary.Cat(main);
    secondary.Cat(main);
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
inline void MString::MetaphAdd(const char main)
{
    primary += main;
    secondary += main;
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
inline void MString::MetaphAdd(const char* main, const char* alt)
{
    if(*main)
        primary.Cat(main);
    if(*alt)
    {
        alternate = true;
        if(alt[0] != ' ')
            secondary.Cat(alt);
    }else
        if(*main AND (main[0] != ' '))
            secondary.Cat(main);
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
bool MString::IsVowel(int at)
{

    if((at < 0) OR (at >= length))
        return false;

    char it = GetAt(at);

    if((it == 'A') OR (it == 'E') OR (it == 'I') OR (it == 'O') OR (it == 'U') OR (it == 'Y') )
        return true;

    return false;
}

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////
bool MString::StringAt(int start, int len, ... )
{

    if (start < 0) return false;

    char    target[64];
    char*   test;

    if (Len - start < len)
    {
        return false;
    }
    memcpy( target, Ptr + start, len );
    target[len] = 0;

    va_list sstrings;
    va_start(sstrings, len);

    do
    {
        test = va_arg(sstrings, char*);
        if(*test AND (strcmp(target, test) == 0))
        {
            va_end(sstrings);
            return true;
        }

    }while(strcmp(test, ""));

    va_end(sstrings);

    return false;
}

////////////////////////////////////////////////////////////////////////////////
// main deal
////////////////////////////////////////////////////////////////////////////////
void MString::DoubleMetaphone(cString &metaph, cString &metaph2)
{

    int current = 0;

    length = Len;
    if(length < 1)
        return;
    last = length - 1;//zero based index

    alternate = false;
    primary = "";
    secondary = "";

    Upper();

    //pad the original string so that we can index beyond the edge of the world
    Cat("     ");

    //skip these when at start of word
    if(StringAt(0, 2, "GN", "KN", "PN", "WR", "PS", ""))
        current += 1;

    //Initial 'X' is pronounced 'Z' e.g. 'Xavier'
    if(GetAt(0) == 'X')
    {
        MetaphAdd('S'); //'Z' maps to 'S'
        current += 1;
    }

    if (isdigit(GetAt(0)))
    {
        while (isdigit(GetAt(current)) && current < length)
        {
            MetaphAdd(GetAt(current));
            current++;
        }
    }
    else while(true OR (primary.Len < 4) OR (secondary.Len < 4))
        ///////////main loop//////////////////////////
    {
        if(current >= length)
            break;

        switch(GetAt(current))
        {
        case 'A':
        case 'E':
        case 'I':
        case 'O':
        case 'U':
        case 'Y':
            if(current == 0)
                //all init vowels now map to 'A'
                MetaphAdd('A');
            current +=1;
            break;

        case 'B':

            //"-mb", e.g", "dumb", already skipped over...
            MetaphAdd('P');

            if(GetAt(current + 1) == 'B')
                current +=2;
            else
                current +=1;
            break;

        case '\307': // ascii 0xc7 = C with cedilla
            MetaphAdd('S');
            current += 1;
            break;

        case 'C':
            //various germanic
            if((current > 1)
                AND !IsVowel(current - 2)
                AND StringAt((current - 1), 3, "ACH", "")
                AND ((GetAt(current + 2) != 'I') AND ((GetAt(current + 2) != 'E')
                OR StringAt((current - 2), 6, "BACHER", "MACHER", "")) ))
            {
                MetaphAdd('K');
                current +=2;
                break;
            }

            //special case 'caesar'
            if((current == 0) AND StringAt(current, 6, "CAESAR", ""))
            {
                MetaphAdd('S');
                current +=2;
                break;
            }

            //italian 'chianti'
            if(StringAt(current, 4, "CHIA", ""))
            {
                MetaphAdd('K');
                current +=2;
                break;
            }

            if(StringAt(current, 2, "CH", ""))
            {
                //find 'michael'
                if((current > 0) AND StringAt(current, 4, "CHAE", ""))
                {
                    MetaphAdd("K", "X");
                    current +=2;
                    break;
                }

                //greek roots e.g. 'chemistry', 'chorus'
                if((current == 0)
                    AND (StringAt((current + 1), 5, "HARAC", "HARIS", "")
                    OR StringAt((current + 1), 3, "HOR", "HYM", "HIA", "HEM", ""))
                    AND !StringAt(0, 5, "CHORE", ""))
                {
                    MetaphAdd('K');
                    current +=2;
                    break;
                }

                //germanic, greek, or otherwise 'ch' for 'kh' sound
                if((StringAt(0, 4, "VAN ", "VON ", "") OR StringAt(0, 3, "SCH", ""))
                    // 'architect but not 'arch', 'orchestra', 'orchid'
                    OR StringAt((current - 2), 6, "ORCHES", "ARCHIT", "ORCHID", "")
                    OR StringAt((current + 2), 1, "T", "S", "")
                    OR ((StringAt((current - 1), 1, "A", "O", "U", "E", "") OR (current == 0))
                    //e.g., 'wachtler', 'wechsler', but not 'tichner'
                    AND StringAt((current + 2), 1, "L", "R", "N", "M", "B", "H", "F", "V", "W", " ", "")))
                {
                    MetaphAdd('K');
                }else{
                    if(current > 0)
                    {
                        if(StringAt(0, 2, "MC", ""))
                            //e.g., "McHugh"
                            MetaphAdd('K');
                        else
                            MetaphAdd("X", "K");
                    }else
                        MetaphAdd('X');
                }
                current +=2;
                break;
            }
            //e.g, 'czerny'
            if(StringAt(current, 2, "CZ", "") AND !StringAt((current - 2), 4, "WICZ", ""))
            {
                MetaphAdd("S", "X");
                current += 2;
                break;
            }

            //e.g., 'focaccia'
            if(StringAt((current + 1), 3, "CIA", ""))
            {
                MetaphAdd('X');
                current += 3;
                break;
            }

            //double 'C', but not if e.g. 'McClellan'
            if(StringAt(current, 2, "CC", "") AND !((current == 1) AND (GetAt(0) == 'M')))
            {
                //'bellocchio' but not 'bacchus'
                if(StringAt((current + 2), 1, "I", "E", "H", "") AND !StringAt((current + 2), 2, "HU", ""))
                {
                    //'accident', 'accede' 'succeed'
                    if(((current == 1) AND (GetAt(current - 1) == 'A'))
                        OR StringAt((current - 1), 5, "UCCEE", "UCCES", ""))
                        MetaphAdd("KS");
                    //'bacci', 'bertucci', other italian
                    else
                        MetaphAdd('X');
                    current += 3;
                    break;
                }else{//Pierce's rule
                    MetaphAdd('K');
                    current += 2;
                    break;
                }
            }

            if(StringAt(current, 2, "CK", "CG", "CQ", ""))
            {
                MetaphAdd('K');
                current += 2;
                break;
            }

            if(StringAt(current, 2, "CI", "CE", "CY", ""))
            {
                //italian vs. english
                if(StringAt(current, 3, "CIO", "CIE", "CIA", ""))
                    MetaphAdd("S", "X");
                else
                    MetaphAdd('S');
                current += 2;
                break;
            }

            //else
            MetaphAdd('K');

            //name sent in 'mac caffrey', 'mac gregor
            if(StringAt((current + 1), 2, " C", " Q", " G", ""))
                current += 3;
            else
                if(StringAt((current + 1), 1, "C", "K", "Q", "")
                    AND !StringAt((current + 1), 2, "CE", "CI", ""))
                    current += 2;
                else
                    current += 1;
            break;

        case 'D':
            if(StringAt(current, 2, "DG", ""))
            {
                if(StringAt((current + 2), 1, "I", "E", "Y", ""))
                {
                    //e.g. 'edge'
                    MetaphAdd('J');
                    current += 3;
                    break;
                }else{
                    //e.g. 'edgar'
                    MetaphAdd("TK");
                    current += 2;
                    break;
                }
            }

            if(StringAt(current, 2, "DT", "DD", ""))
            {
                MetaphAdd('T');
                current += 2;
                break;
            }

            //else
            MetaphAdd('T');
            current += 1;
            break;

       case 'F':
            if(GetAt(current + 1) == 'F')
                current += 2;
            else
                current += 1;
            MetaphAdd('F');
            break;

        case 'G':
            if(GetAt(current + 1) == 'H')
            {
                if((current > 0) AND !IsVowel(current - 1))
                {
                    MetaphAdd('K');
                    current += 2;
                    break;
                }

                if(current < 3)
                {
                    //'ghislane', ghiradelli
                    if(current == 0)
                    {
                        if(GetAt(current + 2) == 'I')
                            MetaphAdd('J');
                        else
                            MetaphAdd('K');
                        current += 2;
                        break;
                    }
                }
                //Parker's rule (with some further refinements) - e.g., 'hugh'
                if(((current > 1) AND StringAt((current - 2), 1, "B", "H", "D", "") )
                    //e.g., 'bough'
                    OR ((current > 2) AND StringAt((current - 3), 1, "B", "H", "D", "") )
                    //e.g., 'broughton'
                    OR ((current > 3) AND StringAt((current - 4), 1, "B", "H", "") ) )
                {
                    current += 2;
                    break;
                }else{
                    //e.g., 'laugh', 'McLaughlin', 'cough', 'gough', 'rough', 'tough'
                    if((current > 2)
                        AND (GetAt(current - 1) == 'U')
                        AND StringAt((current - 3), 1, "C", "G", "L", "R", "T", "") )
                    {
                        MetaphAdd('F');
                    }else
                        if((current > 0) AND GetAt(current - 1) != 'I')
                            MetaphAdd('K');

                        current += 2;
                        break;
                }
            }

            if(GetAt(current + 1) == 'N')
            {
                if((current == 1) AND IsVowel(0) AND !SlavoGermanic())
                {
                    MetaphAdd("KN", "N");
                }else
                    //not e.g. 'cagney'
                    if(!StringAt((current + 2), 2, "EY", "")
                        AND (GetAt(current + 1) != 'Y') AND !SlavoGermanic())
                    {
                        MetaphAdd("N", "KN");
                    }else
                        MetaphAdd("KN");
                    current += 2;
                    break;
            }

            //'tagliaro'
            if(StringAt((current + 1), 2, "LI", "") AND !SlavoGermanic())
            {
                MetaphAdd("KL", "L");
                current += 2;
                break;
            }

            //-ges-,-gep-,-gel-, -gie- at beginning
            if((current == 0)
                AND ((GetAt(current + 1) == 'Y')
                OR StringAt((current + 1), 2, "ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE", "EI", "ER", "")) )
            {
                MetaphAdd("K", "J");
                current += 2;
                break;
            }

            // -ger-,  -gy-
            if((StringAt((current + 1), 2, "ER", "") OR (GetAt(current + 1) == 'Y'))
                AND !StringAt(0, 6, "DANGER", "RANGER", "MANGER", "")
                AND !StringAt((current - 1), 1, "E", "I", "")
                AND !StringAt((current - 1), 3, "RGY", "OGY", "") )
            {
                MetaphAdd("K", "J");
                current += 2;
                break;
            }

            // italian e.g, 'biaggi'
            if(StringAt((current + 1), 1, "E", "I", "Y", "") OR StringAt((current - 1), 4, "AGGI", "OGGI", ""))
            {
                //obvious germanic
                if((StringAt(0, 4, "VAN ", "VON ", "") OR StringAt(0, 3, "SCH", ""))
                    OR StringAt((current + 1), 2, "ET", ""))
                    MetaphAdd('K');
                else
                    //always soft if french ending
                    if(StringAt((current + 1), 4, "IER ", ""))
                        MetaphAdd('J');
                    else
                        MetaphAdd("J", "K");
                    current += 2;
                    break;
            }

            if(GetAt(current + 1) == 'G')
                current += 2;
            else
                current += 1;
            MetaphAdd('K');
            break;

        case 'H':
            //only keep if first & before vowel or btw. 2 vowels
            if(((current == 0) OR IsVowel(current - 1))
                AND IsVowel(current + 1))
            {
                MetaphAdd('H');
                current += 2;
            }else//also takes care of 'HH'
                current += 1;
            break;

        case 'J':
            //obvious spanish, 'jose', 'san jacinto'
            if(StringAt(current, 4, "JOSE", "") OR StringAt(0, 4, "SAN ", "") )
            {
                if(((current == 0) AND (GetAt(current + 4) == ' ')) OR StringAt(0, 4, "SAN ", "") )
                    MetaphAdd('H');
                else
                {
                    MetaphAdd("J", "H");
                }
                current +=1;
                break;
            }

            if((current == 0) AND !StringAt(current, 4, "JOSE", ""))
                MetaphAdd("J", "A");//Yankelovich/Jankelowicz
            else
                //spanish pron. of e.g. 'bajador'
                if(IsVowel(current - 1)
                    AND !SlavoGermanic()
                    AND ((GetAt(current + 1) == 'A') OR (GetAt(current + 1) == 'O')))
                    MetaphAdd("J", "H");
                else
                    if(current == last)
                        MetaphAdd("J", " ");
                    else
                        if(!StringAt((current + 1), 1, "L", "T", "K", "S", "N", "M", "B", "Z", "")
                            AND !StringAt((current - 1), 1, "S", "K", "L", ""))
                            MetaphAdd('J');

                        if(GetAt(current + 1) == 'J')//it could happen!
                            current += 2;
                        else
                            current += 1;
                        break;

        case 'K':
            if(GetAt(current + 1) == 'K')
                current += 2;
            else
                current += 1;
            MetaphAdd('K');
            break;

        case 'L':
            if(GetAt(current + 1) == 'L')
            {
                //spanish e.g. 'cabrillo', 'gallegos'
                if(((current == (length - 3))
                    AND StringAt((current - 1), 4, "ILLO", "ILLA", "ALLE", ""))
                    OR ((StringAt((last - 1), 2, "AS", "OS", "") OR StringAt(last, 1, "A", "O", ""))
                    AND StringAt((current - 1), 4, "ALLE", "")) )
                {
                    MetaphAdd("L", " ");
                    current += 2;
                    break;
                }
                current += 2;
            }else
                current += 1;
            MetaphAdd('L');
            break;

        case 'M':
            if((StringAt((current - 1), 3, "UMB", "")
                AND (((current + 1) == last) OR StringAt((current + 2), 2, "ER", "")))
                //'dumb','thumb'
                OR  (GetAt(current + 1) == 'M') )
                current += 2;
            else
                current += 1;
            MetaphAdd('M');
            break;

        case 'N':
            if(GetAt(current + 1) == 'N')
                current += 2;
            else
                current += 1;
            MetaphAdd('N');
            break;

        case '\321': // Ascii 0xD1 = capital N with tilde
            current += 1;
            MetaphAdd('N');
            break;

        case 'P':
            if(GetAt(current + 1) == 'H')
            {
                MetaphAdd('F');
                current += 2;
                break;
            }

            //also account for "campbell", "raspberry"
            if(StringAt((current + 1), 1, "P", "B", ""))
                current += 2;
            else
                current += 1;
            MetaphAdd('P');
            break;

        case 'Q':
            if(GetAt(current + 1) == 'Q')
                current += 2;
            else
                current += 1;
            MetaphAdd('K');
            break;

        case 'R':
            //french e.g. 'rogier', but exclude 'hochmeier'
            if((current == last)
                AND !SlavoGermanic()
                AND StringAt((current - 2), 2, "IE", "")
                AND !StringAt((current - 4), 2, "ME", "MA", ""))
                MetaphAdd("", "R");
            else
                MetaphAdd('R');

            if(GetAt(current + 1) == 'R')
                current += 2;
            else
                current += 1;
            break;

        case 'S':
            //special cases 'island', 'isle', 'carlisle', 'carlysle'
            if(StringAt((current - 1), 3, "ISL", "YSL", ""))
            {
                current += 1;
                break;
            }

            //special case 'sugar-'
            if((current == 0) AND StringAt(current, 5, "SUGAR", ""))
            {
                MetaphAdd("X", "S");
                current += 1;
                break;
            }

            if(StringAt(current, 2, "SH", ""))
            {
                //germanic
                if(StringAt((current + 1), 4, "HEIM", "HOEK", "HOLM", "HOLZ", ""))
                    MetaphAdd('S');
                else
                    MetaphAdd('X');
                current += 2;
                break;
            }

            //italian & armenian
            if(StringAt(current, 3, "SIO", "SIA", "") OR StringAt(current, 4, "SIAN", ""))
            {
                if(!SlavoGermanic())
                    MetaphAdd("S", "X");
                else
                    MetaphAdd('S');
                current += 3;
                break;
            }

            //german & anglicisations, e.g. 'smith' match 'schmidt', 'snider' match 'schneider'
            //also, -sz- in slavic language altho in hungarian it is pronounced 's'
            if(((current == 0)
                AND StringAt((current + 1), 1, "M", "N", "L", "W", ""))
                OR StringAt((current + 1), 1, "Z", ""))
            {
                MetaphAdd("S", "X");
                if(StringAt((current + 1), 1, "Z", ""))
                    current += 2;
                else
                    current += 1;
                break;
            }

            if(StringAt(current, 2, "SC", ""))
            {
                //Schlesinger's rule
                if(GetAt(current + 2) == 'H')
                {
                    //dutch origin, e.g. 'school', 'schooner'
                    if(StringAt((current + 3), 2, "OO", "ER", "EN", "UY", "ED", "EM", ""))
                    {
                        //'schermerhorn', 'schenker'
                        if(StringAt((current + 3), 2, "ER", "EN", ""))
                        {
                            MetaphAdd("X", "SK");
                        }else
                            MetaphAdd("SK");
                        current += 3;
                        break;
                    }else{
                        if((current == 0) AND !IsVowel(3) AND (GetAt(3) != 'W'))
                            MetaphAdd("X", "S");
                        else
                            MetaphAdd('X');
                        current += 3;
                        break;
                    }
                }

                if(StringAt((current + 2), 1, "I", "E", "Y", ""))
                {
                    MetaphAdd('S');
                    current += 3;
                    break;
                }
                //else
                MetaphAdd("SK");
                current += 3;
                break;
            }

            //french e.g. 'resnais', 'artois'
            if((current == last) AND StringAt((current - 2), 2, "AI", "OI", ""))
                MetaphAdd("", "S");
            else
                MetaphAdd('S');

            if(StringAt((current + 1), 1, "S", "Z", ""))
                current += 2;
            else
                current += 1;
            break;

        case 'T':
            if(StringAt(current, 4, "TION", ""))
            {
                MetaphAdd('X');
                current += 3;
                break;
            }

            if(StringAt(current, 3, "TIA", "TCH", ""))
            {
                MetaphAdd('X');
                current += 3;
                break;
            }

            if(StringAt(current, 2, "TH", "")
                OR StringAt(current, 3, "TTH", ""))
            {
                //special case 'thomas', 'thames' or germanic
                if(StringAt((current + 2), 2, "OM", "AM", "")
                    OR StringAt(0, 4, "VAN ", "VON ", "")
                    OR StringAt(0, 3, "SCH", ""))
                {
                    MetaphAdd('T');
                }else{
                    MetaphAdd("0", "T");
                }
                current += 2;
                break;
            }

            if(StringAt((current + 1), 1, "T", "D", ""))
                current += 2;
            else
                current += 1;
            MetaphAdd('T');
            break;

        case 'V':
            if(GetAt(current + 1) == 'V')
                current += 2;
            else
                current += 1;
            MetaphAdd('F');
            break;

        case 'W':
            //can also be in middle of word
            if(StringAt(current, 2, "WR", ""))
            {
                MetaphAdd('R');
                current += 2;
                break;
            }

            if((current == 0)
                AND (IsVowel(current + 1) OR StringAt(current, 2, "WH", "")))
            {
                //Wasserman should match Vasserman
                if(IsVowel(current + 1))
                    MetaphAdd("A", "F");
                else
                    //need Uomo to match Womo
                    MetaphAdd('A');
            }

            //Arnow should match Arnoff
            if(((current == last) AND IsVowel(current - 1))
                OR StringAt((current - 1), 5, "EWSKI", "EWSKY", "OWSKI", "OWSKY", "")
                OR StringAt(0, 3, "SCH", ""))
            {
                MetaphAdd("", "F");
                current +=1;
                break;
            }

            //polish e.g. 'filipowicz'
            if(StringAt(current, 4, "WICZ", "WITZ", ""))
            {
                MetaphAdd("TS", "FX");
                current +=4;
                break;
            }

            //else skip it
            current +=1;
            break;

        case 'X':
            //french e.g. breaux
            if(!((current == last)
                AND (StringAt((current - 3), 3, "IAU", "EAU", "")
                OR StringAt((current - 2), 2, "AU", "OU", ""))) )
                MetaphAdd("KS");

            if(StringAt((current + 1), 1, "C", "X", ""))
                current += 2;
            else
                current += 1;
            break;

        case 'Z':
            //chinese pinyin e.g. 'zhao'
            if(GetAt(current + 1) == 'H')
            {
                MetaphAdd('J');
                current += 2;
                break;
            }else
                if(StringAt((current + 1), 2, "ZO", "ZI", "ZA", "")
                    OR (SlavoGermanic() AND ((current > 0) AND GetAt(current - 1) != 'T')))
                {
                    MetaphAdd("S", "TS");
                }
                else
                    MetaphAdd('S');

                if(GetAt(current + 1) == 'Z')
                    current += 2;
                else
                    current += 1;
                break;

        default:
            current += 1;
        }
    }

    metaph = primary.Ptr;
    //only give back 4 char metaph
    //if(metaph.Len > 4)
    //        metaph.SetAt(4,'\0');
    metaph2 = secondary.Ptr;
    //if(metaph2.Len > 4)
    //        metaph2.SetAt(4,'\0');

}

}//namespace
