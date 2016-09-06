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
#ifndef FORMATTYPES_HPP
#define FORMATTYPES_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "xslprocessor.hpp"

interface CFormatType  : implements IFormatType, public CInterface
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
