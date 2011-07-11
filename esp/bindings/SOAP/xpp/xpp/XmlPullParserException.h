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
