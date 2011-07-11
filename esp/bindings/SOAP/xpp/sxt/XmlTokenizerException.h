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
#ifndef _XML_TOKENIZER_EXCEPTION_H__
#define _XML_TOKENIZER_EXCEPTION_H__

#include <string>
#include <exception>

using namespace std;

namespace sxt {

  class XmlTokenizerException  : public exception{
  public:
    //XmlTokenizerException() throw() : message(string("XmlTokenizerException")) {}
    XmlTokenizerException(string exMessage) throw() 
      : message(exMessage) {}

    XmlTokenizerException(string exMessage, int exRow, int exColumn) throw() 
      : message(exMessage), row(exRow), column(exColumn) {}

    XmlTokenizerException(const XmlTokenizerException& other) throw() 
      : exception (other)
     {  message       = other.message; }

    XmlTokenizerException& operator=(const XmlTokenizerException& other)
     throw()
    { 
      exception::operator= (other);

      if (&other != this) {
        message = other.message; 
      }

      return *this; 
    }

    virtual ~XmlTokenizerException() throw() 
    {
    }

    int getLineNumber() const { return row; }
    int getColumnNumber() const { return column; }

    string getMessage() const throw() { return message; }
    void setMessage(string exMessage) throw() { message = exMessage;}
    
    virtual const char* what() const throw() {
      return message.c_str();
    }
          

    friend ostream& operator<<(ostream& output, 
       const XmlTokenizerException& xte);

  protected:
    string message;
    int row;
    int column;
  };

inline ostream& operator<<(ostream& output, 
  const XmlTokenizerException& xte) 
{
    output << "XmlTokenizerException: ";
    output << xte.message << endl;
    return output;
}


} //namespace
#endif // _XML_TOKENIZER_EXCEPTION_H__
