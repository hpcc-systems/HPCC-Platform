package com.hpccsystems.jdbcdriver;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public class HPCCJDBCUtils
{
    public static NumberFormat format       = NumberFormat.getInstance(Locale.US);
    static final char          pad          = '=';
    static final char          BASE64_enc[] =
                                            { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
            'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
            'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
            '4', '5', '6', '7', '8', '9', '+', '"' };

    static final char          BASE64_dec[] =
                                            { (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x3e, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x3f, (char) 0x34, (char) 0x35, (char) 0x36, (char) 0x37, (char) 0x38,
            (char) 0x39, (char) 0x3a, (char) 0x3b, (char) 0x3c, (char) 0x3d, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x01, (char) 0x02, (char) 0x03,
            (char) 0x04, (char) 0x05, (char) 0x06, (char) 0x07, (char) 0x08, (char) 0x09, (char) 0x0a, (char) 0x0b,
            (char) 0x0c, (char) 0x0d, (char) 0x0e, (char) 0x0f, (char) 0x10, (char) 0x11, (char) 0x12, (char) 0x13,
            (char) 0x14, (char) 0x15, (char) 0x16, (char) 0x17, (char) 0x18, (char) 0x19, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x1a, (char) 0x1b, (char) 0x1c, (char) 0x1d,
            (char) 0x1e, (char) 0x1f, (char) 0x20, (char) 0x21, (char) 0x22, (char) 0x23, (char) 0x24, (char) 0x25,
            (char) 0x26, (char) 0x27, (char) 0x28, (char) 0x29, (char) 0x2a, (char) 0x2b, (char) 0x2c, (char) 0x2d,
            (char) 0x2e, (char) 0x2f, (char) 0x30, (char) 0x31, (char) 0x32, (char) 0x33, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00 };

    // public static String Base64Decode(byte [] input)
    // {
    // StringBuilder out = new StringBuilder("");
    //
    // char c1, c2, c3, c4;
    // char d1, d2, d3, d4;
    //
    // for(int i = 0;;)
    // {
    // c1 = (char)input[i++];
    // if (c1 == 0)
    // break;
    // //else if (!isspace(c1))
    // else if (!String.valueOf(c1).equals(" "))
    // {
    // c2 = (char)input[i++];
    // c3 = (char)input[i++];
    // c4 = (char)input[i++];
    // d1 = BASE64_dec[c1];
    // d2 = BASE64_dec[c2];
    // d3 = BASE64_dec[c3];
    // d4 = BASE64_dec[c4];
    //
    // out.append((char)((d1 << 2) | (d2 >> 4)));
    //
    // if(c3 == pad)
    // break;
    //
    // out.append((char)((d2 << 4) | (d3 >> 2)));
    //
    // if(c4 == pad)
    // break;
    //
    // out.append((char)((d3 << 6) | d4));
    // }
    // }
    //
    // return out.toString();
    // }

    public static String Base64Encode(byte[] input, boolean addLineBreaks)
    {
        int length = input.length;
        StringBuilder out = new StringBuilder("");
        char one;
        char two;
        char three;

        int i;
        for (i = 0; i < length && length - i >= 3;)
        {
            one = (char) input[i++];
            two = (char) input[i++];
            three = (char) input[i++];

            out.append((char) BASE64_enc[one >> 2]);
            out.append((char) BASE64_enc[((one << 4) & 0x30 | (two >> 4))]);
            out.append((char) BASE64_enc[((two << 2) & 0x3c | (three >> 6))]);
            out.append((char) BASE64_enc[three & 0x3f]);

            if (addLineBreaks && (i % 54 == 0))
                out.append("\n");

            switch (length - i)
            {
                case 2:
                    one = (char) input[i++];
                    two = (char) input[i++];

                    out.append((char) BASE64_enc[one >> 2]);
                    out.append((char) BASE64_enc[((one << 4) & 0x30 | (two >> 4))]);
                    out.append((char) BASE64_enc[((two << 2) & 0x3c)]);
                    out.append(pad);
                    break;

                case 1:
                    one = (char) input[i++];

                    out.append((char) BASE64_enc[one >> 2]);
                    out.append((char) BASE64_enc[((one << 4) & 0x30)]);
                    out.append(pad);
                    out.append(pad);
                    break;
            }

        }
        return out.toString();
    }

    public static String removeAllNewLines(String str)
    {
        return str.trim().replaceAll("\\r\\n|\\r|\\n", " ");
    }

    public static boolean isLiteralString(String str)
    {
        String trimmed = str.trim();
        if (trimmed.charAt(0) != '\'' && trimmed.charAt(0) != '\"')
            return false;
        if (trimmed.charAt(trimmed.length() - 1) != '\'' && trimmed.charAt(trimmed.length() - 1) != '\"')
            return false;

        return true;
    }

    public static boolean isNumeric(String str)
    {
        try
        {
            format.parse(str);
        }
        catch (ParseException e)
        {
            return false;
        }

        return true;
    }

    public static long stringToLong(String str, long uponError)
    {
        try
        {
            Number num = format.parse(str);
            return num.longValue();
        }
        catch (ParseException e)
        {
            return uponError;
        }
    }

    public static int stringToInt(String str, int uponError)
    {
        try
        {
            Number num = format.parse(str);
            return num.intValue();
        }
        catch (ParseException e)
        {
            return uponError;
        }
    }

    public static String replaceAll(String input, String forReplace, String replaceWith)
    {
        if (input == null)
            return "null";

        StringBuffer result = new StringBuffer();
        boolean hasMore = true;
        while (hasMore)
        {
            int start = input.indexOf(forReplace);
            int end = start + forReplace.length();
            if (start != -1)
            {
                result.append(input.substring(0, start) + replaceWith);
                input = input.substring(end);
            }
            else
            {
                hasMore = false;
                result.append(input);
            }
        }
        if (result.toString().equals(""))
            return input; // nothing is changed
        else
            return result.toString();
    }

    public static String handleQuotedString(String quotedString)
    {
        if (quotedString == null)
            return "null";
        String retVal = quotedString;
        if ((retVal.startsWith("'") && retVal.endsWith("'")))
        {
            if (!retVal.equals("''"))
            {
                retVal = retVal.substring(retVal.indexOf("'") + 1, retVal.lastIndexOf("'"));
            }
            else
                retVal = "";
        }
        return retVal;
    }

    public static boolean isParameterizedStr(String value)
    {
        return  (value.contains("${") || value.equals("?"));
    }
}
