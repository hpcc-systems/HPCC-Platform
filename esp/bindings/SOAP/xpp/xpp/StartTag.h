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
#ifndef XPP_START_TAG_H_
#define XPP_START_TAG_H_

#include <string>
#include <xpp/XmlPullParser.h>

/**
 * Encapsulate XML STag and EmptyElement
 * 
 *
 * @author Aleksander Slominski [aslom@extreme.indiana.edu]
 */

//TODO: eliminate class string as it is a big performance hog :-)

namespace xpp {

  class StartTag {
    friend class XmlPullParser;

  public:
    StartTag() { init(); }

    ~StartTag() {
      if(attArr != NULL) {
        delete [] attArr;
      }
      attArr = NULL;
    }

    void clear () { 
      reset(); 
    }

    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getUri() const { return uri; }
    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getLocalName() const { return localName; }
    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getQName() const { return qName; }

    int getLength () { return attEnd; }

    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getURI(int index) const {
      if (index >= 0 && index < attEnd) {
          return attArr[index].uri.c_str();
      } else {
        return NULL;
      }
    }

    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getLocalName(int index) const {
      if (index >= 0 && index < attEnd) {
          return attArr[index].localName.c_str();
      } else {
        return NULL;
      }
    }

    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getRawName(int index) const {
      if (index >= 0 && index < attEnd) {
          return attArr[index].qName.c_str();
      } else {
        return NULL;
      }
    }

    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getType(int index) const {
        index;
      return _MYT("CDATA");
    }


    /** @return DO NOT DEALLOCATE RETURN VALUE! */
    const SXT_CHAR* getValue (int index) const {
      if (index >= 0 && index < attEnd) {
          return attArr[index].value.c_str();
      } else {
        return NULL;
      }
    }

    /** 
     * always return "CDATA" 
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getType (const SXT_CHAR* qName_) const {
      qName_;
      return _MYT("CDATA");
    }
    
    /** 
     * always return "CDATA" 
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getType (const SXT_STRING* uri_, const SXT_CHAR* localName_) const {
      uri_;
      localName_;
      return _MYT("CDATA");
    }

    /** 
     *  
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getValue (const SXT_CHAR* uri_, const SXT_CHAR* localName_) const {
      for(int i = 0; i < attEnd; ++i) {
        if(attArr[i].prefixValid && attArr[i].uri == uri_
           && localName_ == attArr[i].localName)
          {
            return attArr[i].value.c_str();
          }
      }
      return NULL;
    }

    /** 
     *  
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getValue (const SXT_CHAR* qName_) const {
      for(int i = 0; i < attEnd; ++i) {
        if(qName_ == attArr[i].qName)
          {
            return attArr[i].value.c_str();
          }
      }
      return NULL;
    }

    const SXT_STRING toString() const {
      SXT_STRING buf = SXT_STRING("StartTag={");    
      buf = buf + " '" + qName + "'";
      if(SXT_STRING(_MYT("")) != uri) {
        buf = buf + "('" + uri +"','" + localName + "') ";
      }      
      if(attEnd > 0) {
        buf += " attArr=[ ";    
        for(int i = 0; i < attEnd; ++i) {
          SXT_STRING s = attArr[i].toString();
          buf += s + " ";    
        }
        buf += " ]";    
      }
      buf += " }";
      return buf;
    }

  private:

  // ==== utility method
  
  void ensureCapacity(int size) {
    int newSize = 2 * size;
    if(newSize == 0)
      newSize = 25;
    if(attSize < newSize) {
      Attribute* newAttArr = new Attribute[newSize];
      if(attArr != NULL) {
        for(int i = 0; i < attEnd; ++i) {
          newAttArr[i] = attArr[i];
        }
        delete [] attArr;
        attArr = NULL;
      }
      // in C++ it is initialized by default
      //for(int i = attEnd; i < newSize; ++i) {
      //  newAttArr[i] = Attribute();
      //}
      attArr = newAttArr;
      attSize = newSize;
    }
  }
      
    void init() {
      uri = NULL;
      localName = NULL;
      qName = NULL;
      attEnd = attSize = 0;
      attArr = NULL;
    }

    void reset() {
      attEnd = 0;
    }
    
    // ===== internals
               
  protected:
    
    friend ostream& operator<<(ostream& output, 
      const StartTag& startTag);
          

    const SXT_CHAR *uri;
    const SXT_CHAR *localName;
    const SXT_CHAR *qName;

    class Attribute { 
      friend class XmlPullParser;
      friend class StartTag;
    public:
      Attribute() {
        //uri = "";
        //localName = "";
        //qName = "";
        //prefix = "";
        prefixValid = false;
        //value = "";
      }

      Attribute const & operator=(Attribute const &other) {
        if(this != &other) {
          uri = other.uri;
          localName = other.localName;
          qName = other.qName;
          prefix = other.prefix;
          prefixValid = other.prefixValid;
          value = other.value;
        }
        return (*this);
      }

      const SXT_STRING toString() const {
        SXT_STRING buf = SXT_STRING();
        buf = buf + " '" + qName + "'";
        if(prefixValid) {
          buf = buf + "('" + uri +"','" + localName + "')";
        }
        buf = buf + "='" + value + "' ";
        return buf;
      }

    protected:

      SXT_STRING uri;
      SXT_STRING localName;
      SXT_STRING qName;
      SXT_STRING prefix;
      bool prefixValid;
      SXT_STRING value;
      
    };

    int attEnd;
    int attSize;
    Attribute* attArr;
  };

inline ostream& operator<<(ostream& output, 
  const StartTag& startTag) 
{
  const SXT_STRING s = startTag.toString();
  output << s << endl;
  return output;
}


} // namespace

#endif // XPP_START_TAG_H_
