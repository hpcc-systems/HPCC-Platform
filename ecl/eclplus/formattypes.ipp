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
#ifndef FORMATTYPES_HPP
#define FORMATTYPES_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "xslprocessor.hpp"

interface CFormatType  : public CInterface, implements IFormatType
{
protected: 
    int startRowNumber;

public:
    IMPLEMENT_IINTERFACE;
    CFormatType()
    {
        startRowNumber = 0;
    }

    virtual const char * getDataDelimiter()      { return "";    }
    virtual const char * getValueSeparator()     { return "";    }
    virtual const char * getNameValueSeparator() { return "";    }
    virtual const char * getRecordSeparator()    { return "";  }
    virtual bool displayNamesHeader()            { return false; }
    virtual bool embedNames()                    { return false; }
    virtual bool displayRecordNumber()           { return false; }
    virtual bool displayQueryNumber()            { return false; }
    virtual bool displayKeys()                   { return true;  }
    virtual bool getDisplayBoolean()             { return true;  }
    virtual void setStartRowNumber(int num)      { startRowNumber = num;}
    virtual int getStartRowNumber()              { return startRowNumber; }
};


class TextFormatType : public CFormatType
{
private:
    IXslProcessor* xslprocessor;
    
public:
    TextFormatType() { xslprocessor = getXslProcessor();}
    virtual void printBody(FILE* fp, int len, char* txt);
    virtual void printHeader(FILE* fp, const char* name);
    virtual void printFooter(FILE* fp);

    virtual bool isBinary() { return false; }
};

class CSVFormatType : public TextFormatType
{
public:
    virtual const char * getDataDelimiter()      { return "\"";  }
    virtual const char * getValueSeparator()     { return ",";   }
    virtual const char * getNameValueSeparator() { return "";    }
    virtual const char * getRecordSeparator()    { return "\n";  }
    virtual bool displayNamesHeader()            { return false; }
    virtual bool embedNames()                    { return false; }
    virtual bool displayRecordNumber()           { return false; }
    virtual bool displayQueryNumber()            { return true;  }
    virtual bool displayKeys()                   { return false; }
    virtual bool getDisplayBoolean()             { return true;  }
};

class CSVHFormatType : public TextFormatType
{
public:
    virtual const char * getDataDelimiter()      { return "\"";  }
    virtual const char * getValueSeparator()     { return ",";   }
    virtual const char * getNameValueSeparator() { return "";    }
    virtual const char * getRecordSeparator()    { return "\n";  }
    virtual bool displayNamesHeader()            { return true;  }
    virtual bool embedNames()                    { return false; }
    virtual bool displayRecordNumber()           { return false; }
    virtual bool displayQueryNumber()            { return true;  }
    virtual bool displayKeys()                   { return false; }
    virtual bool getDisplayBoolean()             { return true;  }
};

class DefaultFormatType : public TextFormatType
{
public:
    virtual const char * getDataDelimiter()      { return "";    }
    virtual const char * getValueSeparator()     { return " ";   }
    virtual const char * getNameValueSeparator() { return "";    }
    virtual const char * getRecordSeparator()    { return "\n";  }
    virtual bool displayNamesHeader()            { return true;  }
    virtual bool embedNames()                    { return false; }
    virtual bool displayRecordNumber()           { return false; }
    virtual bool displayQueryNumber()            { return true; }
    virtual bool displayKeys()                   { return false; }
    virtual bool getDisplayBoolean()             { return true;  }
};

class RuneclFormatType : public TextFormatType
{
public:
    virtual const char * getDataDelimiter()      { return "";     }
    virtual const char * getValueSeparator()     { return "\n";   }
    virtual const char * getNameValueSeparator() { return " -> "; }
    virtual const char * getRecordSeparator()    { return "\n";   }
    virtual bool displayNamesHeader()            { return false;  }
    virtual bool embedNames()                    { return true;   }
    virtual bool displayRecordNumber()           { return true;   }
    virtual bool displayQueryNumber()            { return true;   }
    virtual bool displayKeys()                   { return false;  }
    virtual bool getDisplayBoolean()             { return true;   }
};

class XmlFormatType : public CFormatType
{
public:
    virtual void printBody(FILE* fp, int len, char* txt);
    virtual void printHeader(FILE* fp, const char* name);
    virtual void printFooter(FILE* fp);

    virtual bool isBinary()                      { return false;  }
};

class BinFormatType : public CFormatType
{
public:
    virtual void printBody(FILE* fp, int len, char* txt);
    virtual void printHeader(FILE* fp, const char* name);
    virtual void printFooter(FILE* fp);

    virtual bool isBinary()                      { return true;  }
};

#endif // FORMATTYPES_HPP
