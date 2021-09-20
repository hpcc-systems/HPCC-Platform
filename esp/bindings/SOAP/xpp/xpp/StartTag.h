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
#ifndef XPP_START_TAG_H_
#define XPP_START_TAG_H_

#include <string>
#include <sxt/XmlTokenizer.h>
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
    friend class CXJXPullParser;

  public:
    StartTag() { init(); }

    StartTag(const StartTag& src) {
      *this = src;
    }

    ~StartTag() {
      if(attArr != NULL) {
        delete [] attArr;
      }
      attArr = NULL;
    }

    StartTag& operator = (const StartTag& src) {
      uri = src.uri;
      localName = src.localName;
      qName = src.qName;
      attEnd = attSize = src.attEnd;
      attArr = new Attribute[attEnd];
      for (int idx = 0; idx < attEnd; idx++)
        attArr[idx] = src.attArr[idx];
      return *this;
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
    const SXT_CHAR* getType(int /*index*/) const {
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
    const SXT_CHAR* getType (const SXT_CHAR* /*qName_*/) const {
      return _MYT("CDATA");
    }
    
    /** 
     * always return "CDATA" 
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getType (const SXT_STRING* /*uri_*/, const SXT_CHAR* /*localName_*/) const {
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
    SXT_STRING nameBuf;

    class Attribute { 
      friend class XmlPullParser;
      friend class CXJXPullParser;
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
