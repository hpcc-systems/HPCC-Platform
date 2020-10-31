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
#ifndef XPP_XML_PULL_PARSER_H_
#define XPP_XML_PULL_PARSER_H_

#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <sxt/XmlTokenizerException.h>
#include <sxt/XmlTokenizer.h>
#include <xpp/EndTag.h>
#include <xpp/StartTag.h>
#include <xpp/XmlPullParserException.h>
#include "xjx.hpp"
#include "jliball.hpp"

/**
 * XML Pull Parser (XPP)
 *
 * <p>Advantages:<ul>
 * <li>very simple pull interface - ideal for deserializing XML objects 
 *   (like SOAP)
 * <li>fast and simple (thin wrapper around XmlTokenizer class)
 * <li>lightweigh memory model - minimized memory allocation: 
 *    element content and attributes are only read on explicit  
 *    method calls, both StartTag and EndTag can be reused during parsing
 * <li>by default supports namespaces parsing (can be switched off)
 * <li>support for mixed element content can be explicitly disabled
 * </ul>
 *
 * <p>Limitations: <ul>
 * <li>does not parse DTD (recognizes only predefined entities)
 * <li>this is beta version - may have still bugs :-)
 * </ul>
 *
 * <p>Future improvements: <ul>
 * <li>switch to use SXT_CHAR* instead of string (SXT_STRING) 
 *   for better performance however it will be necessary to implement
 *   simple reference counting
 * <li>rewrite logic to not depend on readStartTag to declare namespaces
 *     and remove mustReadNamespaces flag 
 *     (as it is already in Java verion)
 * <li>implement stream reading in more() as it is now in Java version
 * </ul>
 *
 * @author Aleksander Slominski [aslom@extreme.indiana.edu]
 */

//TODO: remove ElementContent dependencies on buf instead use int offsets 
//TODO        (as in future buf may be sliding window)
//TODO: revamp nsBuf to use int not pointers 
//TODO      (the same for ElementContent and Attribute!)
//TODO: do not store prefixes in nsBuf???
//TODO: how to make StartTag and EndTag values independent of parsing
//TODO      and still have good performance....?

using namespace sxt;

#define XPP_DEBUG false

namespace xpp {
  class XmlPullParser : implements XJXPullParser {
    
      
  // public 
  public:
    enum {
      END_DOCUMENT = 1,
      START_TAG = 2,
      END_TAG = 3,
      CONTENT = 4
    };

    XmlPullParser() 
    {
      init();
    }

    XmlPullParser(const SXT_CHAR* buf, int bufSize) 
    {
        init();
        nsBufSize = bufSize + 1;
        nsBuf = new SXT_CHAR[nsBufSize];
        tokenizer.setInput(buf, bufSize);
    }

        
    ~XmlPullParser() 
    {
      done();
    }

    void setInput(const SXT_CHAR* buf, int bufSize) {
      reset();      
      if(nsBufSize - 1 < bufSize) {
        int newSize = bufSize + 1;
        SXT_CHAR* newNsBuf = new SXT_CHAR[newSize];
        if(nsBuf != NULL) {
          delete [] nsBuf;
        }
        nsBuf = newNsBuf;
        nsBufSize = newSize;
      }
      tokenizer.setInput(buf, bufSize);
    }


    void setMixedContent(bool enable) { 
      tokenizer.setMixedContent(enable);
    }

    const SXT_CHAR* getQNameLocal(const SXT_CHAR* qName) const {
      int i = 0;
      while(qName[i] != _MYT('\0')) {
        if(qName[i] == _MYT(':'))
          return qName + i + 1;
        ++i;
      }
      return qName;
    }

    const SXT_CHAR* getQNameUri(const SXT_CHAR* qName) 
       
    {
      if(elStackDepth == 0) {
        throw XmlPullParserException(
          string("uri can be only determined when parsing started"));
      }
      int i = 0;
      while(qName[i] != _MYT('\0')) {
        if(qName[i] == _MYT(':'))
          break;
        ++i;
      }
      if(qName[i] == _MYT('\0'))   //current default namespace 
        return elStack[elStackDepth-1].defaultNs; 
      string prefix = string(qName, i);
      if(prefix2Ns.find(prefix) != prefix2Ns.end())
        return prefix2Ns[ prefix ];
      else
        return NULL;
    }

    int getNsCount(){return prefix2Ns.size();}
    map< string, const SXT_CHAR* >::iterator getNsBegin(){return prefix2Ns.begin();}
    map< string, const SXT_CHAR* >::iterator getNsEnd(){return prefix2Ns.end();}

    /** 
     * Set support of namespaces. Enabled by default.
     */
    void setSupportNamespaces(bool enable)  {
      if(elStackDepth > 0 || seenRootElement) {
        throw XmlPullParserException(string(
      "namespace support can only be set before parsing element markup"));
      }  
      supportNs = enable;
    }


    bool skipSubTreeEx()  {
      bool hasChildren = false;
      int level = 1;
      StartTag stag;
      int type = XmlPullParser::END_TAG;
      while(level > 0) {
        type = next();
        switch(type) {
        case XmlPullParser::START_TAG:
          readStartTag(stag);
          ++level;
          hasChildren = true;
          break;
        case XmlPullParser::END_TAG:
          --level;
          break;
        }
      }
      return hasChildren;
    }

    //backward compatability
    int skipSubTree() {
      skipSubTreeEx();
      return XmlPullParser::END_TAG;
    }

    const SXT_STRING getPosDesc() const {
      return tokenizer.getPosDesc();
    }

    int getLineNumber() { return tokenizer.getLineNumber(); }
    int getColumnNumber() { return tokenizer.getColumnNumber(); }
   
    int next()  {
     if(mustReadNamespaces) {
       throw XmlPullParserException(
         string("start tag must be read to declare namespaces")
         +" - please always call readStartTag after next()"
         +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
     }
     if(emptyElement) {
       --elStackDepth;
       if(elStackDepth == 0)
         seenRootElement = true;
       closeEndTag();
       emptyElement = false;
       return eventType = END_TAG;
     }  
     try {
       while(true) {
         //SXT_STRING s;
         token = tokenizer.next();

         switch(token) {
           
         case XmlTokenizer::END_DOCUMENT:
           if(elStackDepth > 0) {
             throw XmlPullParserException(string(
               "expected element end tag '")
               +elStack[elStackDepth-1].qName+"'"+tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           }
           return eventType = END_DOCUMENT;
           
         case XmlTokenizer::CONTENT:
           if(elStackDepth > 0) {
             elContent = NULL;
             return eventType =  CONTENT;
           } else if(tokenizer.seenContent) {           
             throw XmlPullParserException(string(
               "only whitespace content allowed outside root element")
               +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           }  
           // we do allow whitespace content outside of root element
           break;
           
         case XmlTokenizer::ETAG_NAME: {
           if(seenRootElement)
             throw XmlPullParserException(string(
               "no markup allowed outside root element")
               +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           const SXT_CHAR* s = tokenizer.buf + tokenizer.posStart;
           const int len = tokenizer.posEnd - tokenizer.posStart;
           tokenizer.buf[tokenizer.posEnd] = _MYT('\0');
           --elStackDepth;
           if(elStackDepth == 0)
             seenRootElement = true;
           if(elStackDepth < 0) 
             throw XmlPullParserException(string(
                "end tag without start stag")+tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           if(SXT_STRNCMP(s, elStack[elStackDepth].qName, len) != 0) {
             throw XmlPullParserException(string(
                "end tag name should be ")
                +elStack[elStackDepth].qName+" not "+s
                +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           }

           closeEndTag();
           return eventType = END_TAG;
         }

         case XmlTokenizer::STAG_NAME: {
           if(seenRootElement)
             throw XmlPullParserException(string(
               "no markup allowed outside root element")
               +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
           beforeAtts = true;
           emptyElement = false;
           const SXT_CHAR* s = tokenizer.buf + tokenizer.posStart;
           //const int len = tokenizer.posEnd - tokenizer.posStart;
           tokenizer.buf[tokenizer.posEnd] = _MYT('\0');
           if(elStackDepth >= elStackSize) {
             ensureCapacity(elStackDepth);
           }  
           ElementContent& el = elStack[elStackDepth];
           el.prefixesEnd = 0;
           el.prefix = NULL;
           el.qName = s;
           //cout << "element stag s=" << s << endl;
           el.prevNsBufPos = nsBufPos;
           el.defaultNs = NULL;
           //el.defaultNsValid = false;
           if(supportNs ) {
             if(tokenizer.posNsColon > 0) { 
               if(tokenizer.nsColonCount > 1)
                 throw XmlPullParserException(string(
                   "only one colon allowed in prefixed element name")
                   +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());            
               //el.prefixValid = true; 
               //el.prefix = string(tokenizer.buf + tokenizer.posStart, );
               int prefixLen = tokenizer.posNsColon - tokenizer.posStart;
               el.prefix = nsBufAdd(
                 tokenizer.buf + tokenizer.posStart, prefixLen);               
               if(XPP_DEBUG) cerr << "adding el.prefix=" << el.prefix
                  << " el.qName=" << el.qName << endl;
               el.localName = tokenizer.buf + tokenizer.posNsColon + 1;
             } else {
               el.localName = s;
             }
             mustReadNamespaces = true;
           } else {
             el.localName = s;
             el.uri = "";
           }
           ++elStackDepth;           
           return eventType = START_TAG;
         }

         // TODO need to merge readStartTag() here (as in Java version...)
         //
         case XmlTokenizer::STAG_END:
          beforeAtts = false;
          break;      
         case XmlTokenizer::EMPTY_ELEMENT:
          emptyElement = true;
          break;
         case XmlTokenizer::ATTR_NAME:
         case XmlTokenizer::ATTR_CONTENT:
           break;
         default:
           throw XmlPullParserException(string("unknown token ")+
             to_string(token));
           
         }


       }
     } catch(XmlTokenizerException ex) {
       throw XmlPullParserException(string("tokenizer exception: ") 
         + ex.getMessage());
     }
     throw XmlPullParserException(string("illegal parser state"));
   }


    const bool whitespaceContent()  {
      if(eventType != CONTENT) {
        throw XmlPullParserException("no content available to read");
      }
      return tokenizer.seenContent == false;  
    }  
  

    const SXT_CHAR* readContent()  {
      if(eventType != CONTENT) {
        throw XmlPullParserException("no content available to read");
      }
      //if(elContentValid == false) {
      //  if(tokenizer.parsedContent)
      //    elContent = string(tokenizer.pc + tokenizer.pcStart, tokenizer.pcEnd - tokenizer.pcStart);
      //  else
      //    elContent = string(tokenizer.buf + tokenizer.posStart, tokenizer.posEnd - tokenizer.posStart);
      //  elContentValid = false;
      //}
      if(elContent == NULL) {
        if(tokenizer.parsedContent) {
          elContent = tokenizer.pc + tokenizer.pcStart; 
          tokenizer.pc [ tokenizer.pcEnd ] = _MYT('\0');
        } else {
          elContent = tokenizer.buf + tokenizer.posStart; 
          tokenizer.buf [ tokenizer.posEnd ] = _MYT('\0');
        }
      }
      return elContent; 
    }
  
  
    void readEndTag(EndTag& etag)  {
      if(eventType != END_TAG)
        throw XmlPullParserException("no end tag available to read");
      etag.qName = elStack[elStackDepth].qName;
      //if(elStack[elStackDepth].prefix != NULL) {
        etag.uri = elStack[elStackDepth].uri;
      //} else {
      //  etag.uri = _MYT("");
      //}
      etag.localName = elStack[elStackDepth].localName;
    }
  
    void readStartTag(StartTag& stag)  {
      if(eventType != START_TAG)
        throw XmlPullParserException(string(
          "no start tag available to read"));
      if(beforeAtts == false)
        throw XmlPullParserException("start tag was already read");
      assert(elStackDepth > 0);
      ElementContent& el = elStack[elStackDepth - 1];
      stag.qName = el.qName;
      stag.localName = el.localName;
      //cout << "stag.qName=" << stag.qName << endl;
      if(XPP_DEBUG && el.prefix != NULL)
        cerr << "readStartTag  el.prefix=" << el.prefix << endl;
      stag.attEnd = 0;
      try {      
        while(token != XmlTokenizer::STAG_END) {
          token = tokenizer.next();
          switch(token) {

            // reconstruct attribute list
          case XmlTokenizer::ATTR_NAME: {
            if(stag.attEnd >= stag.attSize) {
              stag.ensureCapacity(stag.attEnd);
            }
            StartTag::Attribute &att = stag.attArr[stag.attEnd];  // place for next attribute value
            att.qName = string(tokenizer.buf + tokenizer.posStart, 
              tokenizer.posEnd - tokenizer.posStart);
            att.uri = "";  
            if(supportNs && tokenizer.posNsColon > 0) {
              if(tokenizer.nsColonCount > 1)
                throw XmlPullParserException(string(
                  "only one colon allowed in prefixed attribute name")
                  +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
              att.prefix = att.qName.substr(0, 
                tokenizer.posNsColon - tokenizer.posStart);
              att.prefixValid = true;
              att.localName = att.qName.substr(
                 tokenizer.posNsColon - tokenizer.posStart + 1);
            } else {
              att.prefixValid = false;              
              att.localName = att.qName;
            }
          }
          break;

          case XmlTokenizer::ATTR_CONTENT: {
            // place for next attribute value             
            StartTag::Attribute &att = stag.attArr[stag.attEnd];  
            if(tokenizer.parsedContent)
              att.value = SXT_STRING(tokenizer.pc + tokenizer.pcStart, 
                tokenizer.pcEnd - tokenizer.pcStart);
            else
              att.value = SXT_STRING(tokenizer.buf + tokenizer.posStart, 
                tokenizer.posEnd - tokenizer.posStart);
            if(supportNs) {
              if(att.prefixValid && "xmlns" == att.prefix) {
                // add new NS prefix
                if(el.prefixesEnd >= el.prefixesSize) {
                  el.ensureCapacity(el.prefixesEnd);
                }  
                el.prefixes[el.prefixesEnd] = nsBufAdd(att.localName);
                if(prefix2Ns.find( att.localName ) != prefix2Ns.end())
                  el.prefixPrevNs[ el.prefixesEnd ] 
                    = prefix2Ns[ att.localName ];
                else 
                  el.prefixPrevNs[ el.prefixesEnd ] = NULL;

              if(CHECK_ATTRIB_UNIQ) {
                 //NOTE: O(n2) complexity but n is very small...    
                 //System.err.println("checking xmlns name="+ap.localName);
                  for(int i = 0; i < el.prefixesEnd; ++i) {
                    if(att.localName == el.prefixes[i]) {
                      throw XmlPullParserException(
                        string("duplicate xmlns declaration name '")
                         +att.localName+"'"
                         +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
                    }               
                  }
               }


                ++el.prefixesEnd;
                //prefix2Ns[ att.localName ] = new string(att.value);
                prefix2Ns[ att.localName ] = nsBufAdd(att.value);
                if(XPP_DEBUG) cerr << "NS adding prefix="+att.localName 
                  << " = " << prefix2Ns[ att.localName ]  
                  << " el.prefixesEnd=" << el.prefixesEnd << endl;

              } else if(att.qName == "xmlns") {
                if(el.defaultNs != NULL)
                  throw XmlPullParserException(string(
              "default namespace was alredy declared by xmlns attribute")
                    +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());            
                el.defaultNs = nsBufAdd(att.value);
                //el.defaultNsValid =  true;
              } else {
                ++stag.attEnd;
              }
            } else {
                if(CHECK_ATTRIB_UNIQ) {
                //NOTE: O(n2) complexity but n is small...    
                for(int i = 0; i < stag.attEnd; ++i) {
                  if(stag.attArr[i].qName == att.qName) {
                    throw XmlPullParserException(
                      string("duplicate attribute name '")+att.qName+"'"
                      +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
                  }
                }
              }
              ++stag.attEnd;
            }
          }
          break;

        case XmlTokenizer::EMPTY_ELEMENT:
           //emptyElement = true;
           //break;
          emptyElement = true;
          break;
          
        case XmlTokenizer::STAG_END:
           //beforeAtts = false;
           //if(supportNs) {
           //  assert(elStackDepth > 0);
           //  ElementContent& el = elStack[elStackDepth-1];
           //  if(el.defaultNs == NULL) {
           //    if(elStackDepth > 1) {
           //      el.defaultNs = elStack[elStackDepth-2].defaultNs;
           //    } else {
           //      el.defaultNs = "";
             //  } 
            //   //el.defaultNsValid = true;
            // }
            // //if(el.prefixValid == false) {
            // if(el.prefix == NULL) {
            //   el.uri = el.defaultNs;       
            // }
          // }
          // if(emptyElement) {
            // emptyElement = false;
             //return eventType = END_TAG;
           //}
           //break;  
                    
          beforeAtts = false;
          break;      

        default:
          throw XmlPullParserException(string("unknown token ")
            +to_string(token));      
        }
      }
    } catch(XmlTokenizerException ex) {
      throw XmlPullParserException(string("tokenizer exception: ") 
        + ex.getMessage());
    }
    // fix namespaces in element and attributes
    if(supportNs) {
      if(el.defaultNs == NULL) {
        if(elStackDepth > 1) {
          el.defaultNs = elStack[elStackDepth-2].defaultNs;
        } else {
          el.defaultNs = "";
        } 
        //el.defaultNsValid = true;
      }          
      //System.err.println("el default ns="+el.defaultNs);
      if(el.prefix != NULL) {
        //if(prefix2Ns[ el.prefix ] == NULL)
        if(prefix2Ns.find( el.prefix ) == prefix2Ns.end())
          throw XmlPullParserException(string(
            "no namespace for element prefix ")
            +el.prefix
            +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber()); 
        const SXT_CHAR* ps = prefix2Ns[ el.prefix ];
        assert(ps != NULL);
        stag.uri = el.uri = ps; //(*ps).c_str();
        // assert(stag.uri != null);
      } else {
        //stag.uri = "";
        stag.uri = el.uri = el.defaultNs; //.c_str();
        //System.err.println("setting el default uri="+stag.uri);
        stag.localName = el.localName = stag.qName;
      }
      int n = stag.attEnd;
      for(int i = 0; i < n; ++i) {
        if(stag.attArr[i].prefixValid) {
          SXT_STRING pfx = stag.attArr[i].prefix;
          //if(prefix2Ns[ pfx ]  == NULL)
          if(prefix2Ns.find( pfx ) == prefix2Ns.end())
            throw XmlPullParserException(string(
              "no namespace for attribute prefix ")+pfx
              +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber()); 
          stag.attArr[i].uri = prefix2Ns[ pfx ];
        }  else {
          //stag.attArr[i].localName = stag.attArr[i].qName;          
        }
      }    
      if(CHECK_ATTRIB_UNIQ) {
        // only now can check attribute uniquenes 
        //NOTE: O(n2) complexity but n is small...    
        for(int j = 1; j < n ; ++j) {
          StartTag::Attribute &ap = stag.attArr[j];
          //cerr << "checking " << ap.toString() << endl;
          for(int i = 0; i < j; ++i) {
            StartTag::Attribute &other = stag.attArr[i];
            //cerr << "checking with " << other.toString() << endl;
            //cerr << "ap.prefixValid " << ap.prefixValid << endl;
            //cerr << "other.prefixValid " << other.prefixValid << endl;
            //cerr << "1 " << (ap.localName == other.localName) << endl;
            //cerr << "2 " << (ap.prefixValid == false) << endl;
            //cerr << "3 " << (other.prefixValid == false) << endl;
            
            if( (ap.localName == other.localName)
              && ( ( ap.prefixValid 
                     && other.prefixValid 
                     && (ap.uri == other.uri)
                    )
                 || ( (ap.prefixValid == false) && 
                      (other.prefixValid == false)
                    )
                 )
            ) {
               throw XmlPullParserException(
                 string("duplicate attribute name '")+ap.qName+"'"
                 +((ap.prefixValid) ? 
                     string(" (with namespace '")+ap.uri+"')" 
                   : string(""))
                 +tokenizer.getPosDesc(), tokenizer.getLineNumber(), tokenizer.getColumnNumber());
            }
          }
        }
      }
      mustReadNamespaces = false;      
    } else {
      stag.uri = "";
    }   
  }
  // ====== utility methods 


  private:
  
    void init() {    
      nsBuf = NULL;
      elStackDepth = elStackSize = 0;
      elStack = NULL;    
      beforeAtts = false;
      emptyElement = false;
      seenRootElement = false;
      //elContent = "";
      supportNs = true;
      reset();
    }

    void reset() {  
      token = eventType = -1;
      nsBufPos = nsBufSize = 0;
      elStackDepth = 0;
      //if(prefix2Ns["xml"] != NULL)
      //  delete prefix2Ns["xml"];
      prefix2Ns.clear();
      // 4. NC: Prefix Declared
      prefix2Ns["xml"] = "http://www.w3.org/XML/1998/namespace";
      beforeAtts = false;
      emptyElement = false;
      seenRootElement = false;
      elContent = NULL;
      //elContentValid = false;
      mustReadNamespaces = false;
    }

    void done() {
      reset();
      if(elStack != NULL) {
        delete [] elStack;
      }
      elStack =  NULL;
      
      // delete prefix2Ns values
      // delete element prefixes and prefixPrevNs arrays
      if(nsBuf != NULL) 
        delete [] nsBuf;
      nsBuf = NULL;       
      nsBufPos = nsBufSize = 0;
    }



    void closeEndTag() {
           // clean prefixes
           ElementContent& el = elStack[elStackDepth];
           if(XPP_DEBUG)
                 cerr << "NS current el=" << el.qName 
                 << " el.prefixesEnd =" << el.prefixesEnd
                 << endl; 
           if(supportNs && el.prefixesEnd > 0) { //el.prefixes != NULL) {
             for(int i = el.prefixesEnd - 1; i >= 0; --i) {
               //TODO check if memory leak
               //if( prefix2Ns[ el.prefixes[i] ] != NULL )
               //  ; //delete prefix2Ns[ el.prefixes[i] ]; 
               prefix2Ns[ el.prefixes[i] ] = el.prefixPrevNs[i];
               if(XPP_DEBUG && el.prefixPrevNs[i] != NULL)
                 cerr << "NS restoring prefix=" << el.prefixes[i] 
                 << " = " << el.prefixPrevNs[i] << endl;
               assert(el.prefixPrevNs[i] <= nsBuf + el.prevNsBufPos);
               el.prefixPrevNs[i] = NULL;
             }
             el.prefixesEnd = 0;
             nsBufPos = el.prevNsBufPos;
           }           
    
    }
    void ensureCapacity(int size) {
      int newSize = 2 * size;
      if(newSize == 0)
        newSize = 25;
      if(elStackSize < newSize) {
        ElementContent* newStack = new ElementContent[newSize];
        if(elStack != NULL) {
          //System.arraycopy(elStack, 0, newStack, 0, elStackDepth);
          memcpy(newStack, elStack, elStackDepth * sizeof(newStack[0]));
          delete [] elStack;
          elStack = NULL;
        }
        // in C++ it is not necessary
        // for(int i = elStackSize; i < newSize; ++i) {
        //   newStack[i] = new ElementContent();
        //}
        elStack = newStack;
        elStackSize = newSize;
      }
    }

    void ensureNsBufSpace(int addSpace) {
      addSpace = addSpace;
    /*
    // NOTE: unfortunately it can not be used as i was storing char* pointers 
    //   to this block of memory, storing relative offsets though will work - but later!
      if(nsBufPos + addSpace + 1> nsBufSize) {
        int newSize = 2 * nsBufSize;
        if(newSize == 0)
          newSize = 25;
        SXT_CHAR* newNsBuf = new SXT_CHAR[newSize];
        if(nsBuf != NULL) {
          //System.arraycopy(elStack, 0, newStack, 0, elStackDepth);
          memcpy(newNsBuf, nsBuf, nsBufPos * sizeof(nsBuf[0]));
          delete nsBuf;
        }
        nsBuf = newNsBuf;
        nsBufSize = newSize;
      }
    */
    }

    SXT_CHAR* nsBufAdd(const SXT_CHAR* s, int sLen) {
      if(XPP_DEBUG) cerr << "nsBufAdd nsBufPos=" << nsBufPos 
          << " nsBufSize="<< nsBufSize 
          << " s=" << s << " sLen=" << sLen << endl;
      //ensureNsBufSpace(sLen);
      SXT_CHAR* result = nsBuf + nsBufPos;
      memcpy(result, s, sLen * sizeof(SXT_CHAR));
      nsBufPos += sLen + 1;
      nsBuf[nsBufPos - 1] = _MYT('\0');
      return result;
    }

    SXT_CHAR* nsBufAdd(SXT_STRING s) {
      return nsBufAdd(s.c_str(), s.size());
    }
      
    const SXT_STRING to_string(int i) const {
      ostringstream os;
      os << i;
      return os.str();
    }


  // ===== internals  
    private:

      friend ostream& operator<<(ostream& output, 
        const XmlPullParser& xpp);

      enum {
        CHECK_ATTRIB_UNIQ = 1
      };
  
      bool mustReadNamespaces;

          bool beforeAtts;
      bool emptyElement;
      bool seenRootElement;
      const SXT_CHAR* elContent;
      //SXT_STRING elContent;
      //bool elContentValid;

      XmlTokenizer tokenizer;
      int eventType;
      int token;
  
      // mapping namespace prefixes to uri
      bool supportNs;
      //map< SXT_STRING, SXT_STRING*, less<SXT_STRING> > prefix2Ns;
      SXT_CHAR* nsBuf;
      int nsBufPos;
      int nsBufSize;
      map< string, const SXT_CHAR* > prefix2Ns;

      class ElementContent { 
        friend class XmlPullParser;

        const SXT_CHAR* qName;
        const SXT_CHAR* uri;
        const SXT_CHAR* localName;
        const SXT_CHAR* prefix;
        //bool prefixValid;

        const SXT_CHAR* defaultNs;
        int prevNsBufPos;
        //bool defaultNsValid;

        int prefixesEnd;
        int prefixesSize;
        SXT_CHAR** prefixes;
        const SXT_CHAR** prefixPrevNs; 

        ElementContent() {
          //prefixValid = false;
          prefix = NULL;
          prefixesEnd = prefixesSize = 0;
          prefixes = NULL;
          prefixPrevNs = NULL;
          //defaultNsValid = false;
          defaultNs = NULL;
          qName = uri = localName = NULL;
          prevNsBufPos = -1;
        }

        ~ElementContent() {
          if(prefixes != NULL) {
            delete [] prefixes; 
            prefixes = NULL;
            delete [] prefixPrevNs;
            prefixPrevNs = NULL;
          }
        }

      private:
        void ensureCapacity(int size) {
          int newSize = 2 * size;
          if(newSize == 0)
            newSize = 25;
          if(prefixesSize < newSize) {
            SXT_CHAR** newPrefixes = new SXT_CHAR*[newSize];
            const SXT_CHAR** newPrefixPrevNs 
              = new const SXT_CHAR*[newSize];
            if(prefixes != NULL) {
              memcpy(newPrefixes, prefixes, 
                prefixesEnd * sizeof(prefixes[0]));
              //for(int i=0; i < prefixesEnd; ++i) {
              //  newPrefixes[i] = prefixes[i];
              //}
              delete [] prefixes; 
              memcpy(newPrefixPrevNs, prefixPrevNs, 
                prefixesEnd * sizeof(prefixPrevNs[0]));
              delete [] prefixPrevNs;
              for(int j=prefixesEnd; j < newSize; ++j) {
                newPrefixPrevNs[j] = NULL;
              }
            }
            prefixes = newPrefixes;
            prefixPrevNs = newPrefixPrevNs;
            prefixesSize = newSize;
          }
        }

      };

      // for validating element pairing and string namespace context
      int elStackDepth;
      int elStackSize;
      ElementContent* elStack;

  };

inline ostream& operator<<(ostream& output, 
  const XmlPullParser& xpp) 
{
    SXT_STRING ss = xpp.to_string(xpp.eventType);
    if(xpp.eventType == XmlPullParser::END_DOCUMENT) {
      ss = "END_DOCUMENT";
    } else if(xpp.eventType == XmlPullParser::START_TAG) {
      ss = "START_TAG";
    } else if(xpp.eventType == XmlPullParser::END_TAG) {
      ss = "END_TAG";
    } else if(xpp.eventType == XmlPullParser::CONTENT) {
      ss = "CONTENT";
    }   
    SXT_STRING s = "XmlPullParser: current evenType: "+ss;
    output << s << endl;
    return output;
}

inline bool isXppEndType(int type)
{
    return (type==XmlPullParser::END_TAG || type==XmlPullParser::END_DOCUMENT);
}
inline void readFullContent(XmlPullParser &xppx, StringBuffer &content)
{
    StartTag stag;
    for(int type=xppx.next(); !isXppEndType(type); type=xppx.next())
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:
                xppx.readStartTag(stag);
                xppx.skipSubTree();
                break;
            case XmlPullParser::CONTENT:
                content.append(xppx.readContent());
                break;
        }
    }
}


} //namespace

//#pragma warning (pop)
#endif // XPP_XML_PULL_PARSER_H_
