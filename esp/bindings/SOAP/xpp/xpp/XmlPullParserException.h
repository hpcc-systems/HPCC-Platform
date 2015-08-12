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

// -*-c++-*- --------------74-columns-wide-------------------------------|
#ifndef _XML_PULL_PARSER_EXCEPTION_H__
#define _XML_PULL_PARSER_EXCEPTION_H__

#include <string>
#include <exception>
//#include <stdexcept>

using namespace std;

namespace xpp {

  class XmlPullParserException : public exception {
  public:
    //XmlPullParserException() throw() : message(string("XmlPullParserException")) {}
    XmlPullParserException(string exMessage) 
      throw() : message(exMessage) {}

    XmlPullParserException(string exMessage, int exRow, int exColumn) throw() 
      : message(exMessage), row(exRow), column(exColumn) {}



    XmlPullParserException(const XmlPullParserException& other) 
      throw() : exception (other)
     {  message       = other.message; }

    virtual ~XmlPullParserException() throw() {}

    XmlPullParserException& operator=(const XmlPullParserException& other) throw()
    { 
      exception::operator= (other);

      if (&other != this) {
        message       = other.message; 
      }

      return *this; 
    }

    int getLineNumber() const { return row; }
    int getColumnNumber() const { return column; }

    string getMessage() const throw() {return message;}
    void setMessage(string exMessage) throw() { message = exMessage;}
    
    virtual const char* what() const throw() {
      return message.c_str();
    }

  protected:

    friend ostream& operator<<(ostream& output, 
       const XmlPullParserException& xppe);
          

    string message;
    int row;
    int column;    
  };

inline ostream& operator<<(ostream& output, 
  const XmlPullParserException& xppe) 
{
    output << "XmlPullParserException: ";
    output << xppe.message << endl;
    return output;
}


} //namespace

#endif // _XML_PULL_PARSER_EXCEPTION_H__
