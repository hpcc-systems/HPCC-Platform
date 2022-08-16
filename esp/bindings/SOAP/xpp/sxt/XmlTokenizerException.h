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
#ifndef _XML_TOKENIZER_EXCEPTION_H__
#define _XML_TOKENIZER_EXCEPTION_H__

#include <string>
#include <exception>

namespace sxt {

  using std::string;
  using std::exception;
  using std::ostream;

  class XmlTokenizerException  : public exception{
  public:
    XmlTokenizerException(string exMessage) throw() 
      : message(exMessage) {}

    XmlTokenizerException(string exMessage, int exRow, int exColumn) throw() 
      : message(exMessage), row(exRow), column(exColumn) {}

    XmlTokenizerException(const char *exMessage, char ch, const string &posDesc, int exRow, int exColumn) throw()
      : message(exMessage), row(exRow), column(exColumn)
    {
        if (ch)
            message += ch;
        message.append(posDesc);
    }

    XmlTokenizerException(const char *exMessage, const string &posDesc, int exRow, int exColumn) throw()
      : message(exMessage), row(exRow), column(exColumn)
    {
        message.append(posDesc);
    }

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
    output << xte.message << std::endl;
    return output;
}


} //namespace
#endif // _XML_TOKENIZER_EXCEPTION_H__
