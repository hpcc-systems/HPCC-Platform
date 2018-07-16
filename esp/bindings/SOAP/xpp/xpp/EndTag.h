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
#ifndef XPP_END_TAG_H_
#define XPP_END_TAG_H_

#include <string>

#include <sxt/XmlTokenizer.h>

using namespace std;

/**
 * Encapsulate XML ETag
 * 
 *
 * @author Aleksander Slominski [aslom@extreme.indiana.edu]
 */

namespace xpp {

  class EndTag {

    friend class XmlPullParser;
    friend class CXJXPullParser;

  public:
    EndTag() { init(); }
    /** 
     *  
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getUri() const { return uri; }

    /** 
     *  
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getLocalName() const { return localName; }      

    /** 
     *  
     * @return DO NOT DEALLOCATE RETURN VALUE! 
     */
    const SXT_CHAR* getQName() const { return qName; }

    const SXT_STRING toString() const {
      string buf = SXT_STRING("EndTag={");    
      buf = buf + " '" + qName + "'";
      if(SXT_STRING(_MYT("")) != uri) {
        buf = buf + "('" + uri +"','" + localName + "') ";
      }
      buf += " }";
      return buf;
    }
    
  protected:
    void init() {
      uri = NULL;
      localName = NULL;
      qName = NULL;
    }

    friend ostream& operator<<(ostream& output, 
      const EndTag& startTag);
          
               
    // ===== internals available for superclasses
    
    const SXT_CHAR* uri;
    const SXT_CHAR* localName;
    const SXT_CHAR* qName;
    SXT_STRING nameBuf;
  };
  
inline ostream& operator<<(ostream& output, 
  const EndTag& endTag) 
{
    const SXT_STRING s = endTag.toString();
    output << s << endl;
    return output;
}

  
  
} //namespace

#endif // XPP_END_TAG_H_
