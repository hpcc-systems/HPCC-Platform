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
#ifndef XPP_END_TAG_H_
#define XPP_END_TAG_H_

#include <string>

#include <xpp/XmlPullParser.h>

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
