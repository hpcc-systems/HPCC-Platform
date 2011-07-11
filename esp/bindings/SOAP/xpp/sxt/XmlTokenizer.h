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
#ifndef SXT_XML_TOKENIZER_H_
#define SXT_XML_TOKENIZER_H_

#include <assert.h>
#include <sstream>
#include <string>
#include <sxt/XmlTokenizerException.h>

#define SXT_UNICODE 0

#if SXT_UNICODE
typedef wchar_t SXT_CHAR;
#define SXT_STRING wstring
#ifdef _MYT
#undef _MYT
#endif
#define _MYT(str) (L##str)
#else
typedef char SXT_CHAR;
#define SXT_STRING string
#ifdef _MYT
#undef _MYT
#endif
#define _MYT(str) (str)
#define SXT_STRNCMP strncmp
#define SXT_STRLEN strlen
#endif

//#define SXT_TRACING false;
//#define SXT_TEST_VALIDATING  false;

#pragma warning(push)
#pragma warning(disable:4290)

namespace sxt {


/**
 * Simpe XML Tokenizer (SXT)
 * 
 * <p>Advantages:<ul>
 * <li>utility class to simplify creation of XML parsers,
 *     especially suited for pull event model but can support also push
 * <li>minimal memory utilization: does not use memory except for input 
 *    and content buffer (that can grow in size)  
 * <li>fast: all parsing done in one function 
 *       (simple deterministic automata)
 * <li>supports most of XML 1.0 (except validation and external entities)
 * <li>low level: supports on demand parsing of Characters, CDSect, 
 *      Comments, PIs etc.)
 * <li>parsed content: supports providing on demand  
 *     parsed content to application (standard entities expanded
 *    all CDATA sections inserted, Comments and PIs removed)
 *    not for attribute values and element content
 * <li>mixed content: allow to dynamically disable mixed content
 * <li>small - justs one include file!
 * </ul>
 *
 * <p>Limitations:<ul>
 * <li>it is just a tokenizer - does not enforce grammar
 * <li>readName() is using Java identifier rules not XML
 * <li>does not parse DOCTYPE declaration (skips everyting in [...])
 * </ul>
 *
 * <p>Limitations of C++ verision:<ul>
 * <li>it does not support reading from stream (contrary to Java verison)
 *    <strong>but</strong>
 *    whole input is duplicated before parsing and can be used by calling
 *    application (such as xpp) as working buffer up to pos - 1 character
 *    (so it can insert \0 to create null terminated strings). 
 *    this buffer is valid until next setInput().
 *     (however it can be tricky!) 
 * </ul>
 *
 * @author Aleksander Slominski [aslom@extreme.indiana.edu]
 */

  class XmlTokenizer {
  public:
      enum {
            END_DOCUMENT          = 2,
            CONTENT               = 10,
            CHARACTERS            = 20,
            CDSECT                = 30,
            COMMENT               = 40,
            DOCTYPE               = 50,
            PI                    = 60,
            ENTITY_REF            = 70,
            CHAR_REF              = 75,
            ETAG_NAME             = 110,
            EMPTY_ELEMENT         = 111,
            STAG_END              = 112,
            STAG_NAME             = 120,
            ATTR_NAME             = 122,
            ATTR_CHARACTERS       = 124,
            ATTR_CONTENT          = 127
      };


    bool paramNotifyCharacters;
    bool paramNotifyComment;
    bool paramNotifyCDSect;
    bool paramNotifyDoctype;
    bool paramNotifyPI;
    bool paramNotifyCharRef;
    bool paramNotifyEntityRef;
    bool paramNotifyAttValue;

    SXT_CHAR* buf;
    int pos;
    int posStart;
    int posEnd;
    int posNsColon;
    int nsColonCount;
    bool seenContent;

    bool parsedContent;
    SXT_CHAR* pc;
    int pcStart;
    int pcEnd;

  public:
      XmlTokenizer() { init(); }
    ~XmlTokenizer(){ done(); }

    //void setInput(string s);
    void setInput(const SXT_CHAR* buf_, int size) {
      reset();
      this->bufEnd = size;
      if(size > this->bufSize - 1) {
        if(this->buf != NULL)
          delete [] this->buf;
        this->bufSize = bufEnd + 1;  //NOTE: +1 to give place for '\0' 
        this->buf = new SXT_CHAR[this->bufSize]; 
      }
      memcpy(this->buf, buf_, bufEnd * sizeof(this->buf[0]));
      this->buf[bufEnd] = _MYT('\0');
      if(paramPC && bufSize > pcSize) {
        if(pc != NULL)
          delete [] pc;
        pc = new SXT_CHAR[bufSize]; 
        pcSize = bufSize;
      }
      //if(SXT_TEST_VALIDATING) for(int i=0; i < bufSize; ++i) pc[i]='X';
    }

  /**
   * Set notification of all XML content tokens:
   * Characters, Comment, CDSect, Doctype, PI, EntityRef, CharRef, 
   * AttValue (tokens for STag, ETag and Attribute are always sent).
   */
  void setNotifyAll(bool enable) {
    paramNotifyCharacters = enable;
    paramNotifyComment = enable;
    paramNotifyCDSect = enable;
    paramNotifyDoctype = enable;
    paramNotifyPI = enable;
    paramNotifyEntityRef = enable;
    paramNotifyCharRef = enable;
    paramNotifyAttValue = enable;
  }

  /**
   * Allow reporting parsed content for element content
   * and attribute content (no need to deal with low level
   * tokens such as in setNotifyAll).
   */
  void setParseContent(bool enable) {
    paramPC = enable;
    if(paramPC && pcSize < bufSize) {
      pc = new SXT_CHAR[bufSize];
      pcSize = bufSize;
    }
  }

  /**
   * Set support for mixed conetent. If mixed content is
   * disabled tokenizer will do its best to ensure that
   * no element has mixed content model also ignorable whitespaces
   * will not be reported as element content.
   */
  void setMixedContent(bool enable) {
    paramNoMixContent = !enable;
  }


  /**
   * Return next recognized toke or END_DOCUMENT if no more input.
   *
   * <p>This is simple automata (in pseudo-code):
   * <pre>
   * byte next() {
   *    while(state != END_DOCUMENT) {
   *      ch = more();  // read character from input
   *      state = func(ch, state); // do transition
   *      if(state is accepting)
   *        return state;  // return token to caller
   *    }
   * }
   * </pre>
   *
   * <p>For simplicity it is using few inlined procedures 
   *   such as readName() or isS().
   *
   */
   //int XmlTokenizer::next() throw(XmlTokenizerException) {
   int next() throw(XmlTokenizerException) {
     if(state == STATE_FINISHED)
          return END_DOCUMENT;
     parsedContent = false;

//LOOP:
     while(true) {
       if(reachedEnd) {
          if(state != STATE_FINISH) {
            if(state != STATE_CONTENT && state != STATE_CONTENT_INIT 
               && state != STATE_CONTENT_CONTINUED)
              throw XmlTokenizerException(string(
                "unexpected end of stream (state=")+to_string(state)+")");
            if(state == STATE_CONTENT_INIT 
              || state == STATE_CONTENT_CONTINUED) {
              if(state == STATE_CONTENT_INIT) {
                //pcEnd = pcStart = pos - 1;
                pcEnd = pcStart = pcEnd + 1;
              }
              posEnd = posStart = pos - 1;
            }
            state = STATE_FINISH;  
            if(paramPC && (pcStart != pcEnd || posEnd != posStart)) {
              parsedContent = (pcEnd != pcStart);
              //if(pcEnd == pcStart) {
              //  pcStart = posStart;
              //  pcEnd = posEnd;
              //  parsedContent = false;
             // } else {
                //pc = pc;
              //  parsedContent = true;
              //}
              //return CONTENT;
              if(paramNoMixContent == false || seenContent == false)

                return CONTENT;
              else if(parsedContent)
                throw XmlTokenizerException(string(
                  "no element content allowed before end of stream"));
              
            }
          }
          state = STATE_FINISHED;
          return END_DOCUMENT;
       }
       char ch = more();
        //if(TRACING) System.err.println("TRACING XmlTokenizer ch="
        //              +printable(ch)+" state="+to_string(state)
        //              +" posStart="+to_string(posStart)
        //              +" posEnd="+to_string(posEnd)
        //              +" pcStart="+pcStart+" pcEnd="+pcEnd);

       // 2.11 End-of-Line Handling: "\r\n" -> "\n"; "\rX" -> "\nX"
       //if(NORMALIZE_LINE_BREAKS && prevPrevCh == '\r' && ch == '\n')
       //  continue; //ch = more();
      if(NORMALIZE_LINE_BREAKS) {
        if(ch == '\r') {
          // TODO: joinPC()
          if(pcStart == pcEnd && posEnd > posStart) {
            int len = posEnd - posStart;
            //System.arraycopy(buf, posStart, pc, pcEnd, len);
            memcpy(pc + pcEnd, buf + posStart, len * sizeof(SXT_CHAR)); //FIXME
            pcEnd += len;
          }
        } else if(prevPrevCh == '\r' && ch == '\n') {
          continue; //ask for more chars --> ch = more();
        }
      }
       

       switch(state) {
        case STATE_INIT:
          ; // fall through
        case STATE_CONTENT_INIT:
          //pcEnd = pcStart = 0;
          pcEnd = pcStart = pcEnd + 1;
          ; // fall through
        case STATE_CONTENT_CONTINUED:
          posEnd = posStart = pos - 1;
          state = STATE_CONTENT;
          ; // fall through
        case STATE_CONTENT:
          if(ch == '<') {
            state = STATE_SEEN_LT;
            if(paramNotifyCharacters && posStart != posEnd)
              return CHARACTERS;
          } else if(ch == '&') {
            if(paramPC && pcStart == pcEnd && posEnd > posStart) {
              // TODO: joinPC()
              int len = posEnd - posStart;
              //System.arraycopy(buf, posStart, pc, pcEnd, len);
              memcpy(pc + pcEnd, buf + posStart, len * sizeof(SXT_CHAR)); //FIXME
              pcEnd += len;
            }
            if(!seenContent) {
              seenContent = true;
              if(paramNoMixContent && !mixInElement)
                throw XmlTokenizerException(
                   string("mixed content disallowed outside element")                   
                   +getPosDesc(), getLineNumber(), getColumnNumber());
            }
            state = STATE_SEEN_AMP;
            previousState = STATE_CONTENT_CONTINUED;
            posStart = pos - 1;
          } else {
            if(!seenContent && !isS(ch)) {
              seenContent = true;
              if(paramNoMixContent && !mixInElement)
                throw XmlTokenizerException(string(
                  "mixed content disallowed outside element")
                  +"character '"+printable(ch)+"'"
                //+" ("+((int)ch)+")"
                  +getPosDesc(), getLineNumber(), getColumnNumber());
            }
            posEnd = pos;
            //if(paramPC && pcStart != pcEnd)
            //  pc[pcEnd++] = ch;
            if(paramPC && pcStart != pcEnd
              || (paramPC && NORMALIZE_LINE_BREAKS && ch == '\r') 
            ) {
              //XXX
              if(NORMALIZE_LINE_BREAKS && ch == '\r') 
                pc[pcEnd++] = '\n';
              else
                pc[pcEnd++] = ch;
            }
            if(paramNotifyCharacters && reachedEnd)
              return CHARACTERS;
          }
          break;
        case STATE_SEEN_LT:
          if(ch == '!') {
            state = STATE_SEEN_LT_BANG;
          } else if(ch == '?') {
            state = STATE_PI;
          } else { // it must be STag or ETag
            //bool prevMixSeenContent = seenContent;
            bool prevMixInElement = mixInElement;
            bool prevMixSeenContent = seenContent;
            if(ch == _MYT('/')) {
              state = STATE_SCAN_ETAG_NAME;
              mixInElement = false;
            } else {
              //cerr << "ALEK EXC " << " mix=" << seenContent << endl;
              state = STATE_SCAN_STAG_NAME;
              if(paramNoMixContent && seenContent)
                throw XmlTokenizerException(
          "mixed content disallowed inside element and before start tag"
                +getPosDesc(), getLineNumber(), getColumnNumber());
              mixInElement = true;
            }
            if(paramPC /*&& (pcStart != pcEnd || posEnd != posStart)*/) {
              parsedContent = (pcEnd != pcStart);
              if(paramNoMixContent == false
                  || (paramNoMixContent && state == STATE_SCAN_ETAG_NAME
                      //&& prevMixInElement && prevMixSeenContent)) {
                      && prevMixInElement)) {
                return CONTENT;
              }  
            }
          }
          // gather parsed content
          if(paramPC && state != STATE_SCAN_STAG_NAME 
            && state != STATE_SCAN_ETAG_NAME) {
            // TODO: joinPC()
            if(pcStart == pcEnd && posEnd > posStart) {
              int len = posEnd - posStart;
              //System.arraycopy(buf, posStart, pc, pcEnd, len);
              memcpy(pc + pcEnd, buf + posStart, len * sizeof(SXT_CHAR)); //FIXME UNICODE
              pcEnd += len;
            }
          }
          posStart = pos;  // to make PI start content
          break;
        case STATE_SEEN_LT_BANG:
          if(ch == '-') {
            ch = more();
            if(ch != '-')
              throw XmlTokenizerException(
                "expected - for start of comment <!-- not "
                +ch+getPosDesc(), getLineNumber(), getColumnNumber());
            state = STATE_COMMENT;
            posStart = pos;
          } else if(ch == '[') {
            ch = more(); if(ch != 'C') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'D') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'A') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'T') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'A') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != '[') throw XmlTokenizerException(
              "expected <![CDATA"+getPosDesc(), getLineNumber(), getColumnNumber());
            posStart = pos;
            if(!seenContent) {
              seenContent = true;
              if(paramNoMixContent && !mixInElement)
                throw XmlTokenizerException(
                  "mixed content disallowed outside element"+getPosDesc(), getLineNumber(), getColumnNumber());
            }
            state = STATE_CDSECT;
          } else if(ch == 'D') {
            ch = more(); if(ch != 'O') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'C') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'T') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'Y') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'P') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            ch = more(); if(ch != 'E') throw XmlTokenizerException(
              "expected <![DOCTYPE"+getPosDesc(), getLineNumber(), getColumnNumber());
            posStart = pos;
            state = STATE_DOCTYPE;
          } else {
            throw XmlTokenizerException("unknown markup after <! "
              +ch+getPosDesc(), getLineNumber(), getColumnNumber());
          }
          break;

        // [66]-[68] reference ; 4.1 Character and Entity Reference
        case STATE_SEEN_AMP:
          posStart = pos - 2;
          if(ch == '#') {
            state = STATE_CHAR_REF;
            break;
          }
          state = STATE_ENTITY_REF;
          ; // fall through
        case STATE_ENTITY_REF:
          if(ch == ';') {
            state = previousState;
            posEnd = pos;
            // 4.6 Predefined Entities
            if(paramPC) {
              int i = posStart + 1;
              int j = pos - 1;
              int len = j - i;
              if(len == 2 && buf[i] == 'l' && buf[i+1] == 't') {
                pc[pcEnd++] = '<';
              } else if(len == 3 && buf[i] == 'a'
                        && buf[i+1] == 'm' && buf[i+2] == 'p') {
                pc[pcEnd++] = '&';
              } else if(len == 2 && buf[i] == 'g' && buf[i+1] == 't') {
                pc[pcEnd++] = '>';
              } else if(len == 4 && buf[i] == 'a' && buf[i+1] == 'p'
                     && buf[i+2] == 'o' && buf[i+3] == 's')
              {
                pc[pcEnd++] = '\'';
              } else if(len == 4 && buf[i] == 'q' && buf[i+1] == 'u'
                     && buf[i+2] == 'o' && buf[i+3] == 't')
              {
                pc[pcEnd++] = '"';
              } else {
                //String s = new String(buf, i, j - i);
                throw XmlTokenizerException("undefined entity "
                  +getPosDesc(), getLineNumber(), getColumnNumber());
              }
            }
            if(paramNotifyEntityRef)
              return ENTITY_REF;
          }
          break;
        case STATE_CHAR_REF:
          charRefValue = 0;
          state = STATE_CHAR_REF_DIGITS;
          if(ch == 'x') {
            charRefHex = true;
            break;
          }
          charRefHex = false;
          ; // fall through
        case STATE_CHAR_REF_DIGITS:
          if(ch == ';') {
            if(paramPC) {
              pc[pcEnd++] = charRefValue;
            }
            state = previousState;
            posEnd = pos;
            if(paramNotifyCharRef)
              return CHAR_REF;
          } else if(ch >= '0' && ch <= '9') {
            if(charRefHex) {
              charRefValue = (char)(charRefValue * 16 + (ch - '0'));
            } else {
              charRefValue = (char)(charRefValue * 10 + (ch - '0'));
            }
          } else if(charRefHex && ch >= 'A' && ch <= 'F') {
              charRefValue = (char)(charRefValue * 16 + (ch - 'A' + 10));
          } else if(charRefHex && ch >= 'a' && ch <= 'f') {
              charRefValue = (char)(charRefValue * 16 + (ch - 'a' + 10));
          } else {
            throw XmlTokenizerException(
              "character reference may not contain "+ch+getPosDesc(), getLineNumber(), getColumnNumber());
          }
          break;

        // [40]-[44]; 3.1 Start-Tags, End-Tags, and Empty-Element Tags
        case STATE_SCAN_ETAG_NAME:
          seenContent = false;
          posStart = pos - 1;
          ch = readName(ch);
          posEnd = pos - 1;
          ch = skipS(ch);
          if(ch != '>')
            throw XmlTokenizerException("expected > for end tag not "+ch+getPosDesc(), getLineNumber(), getColumnNumber());
          state = STATE_CONTENT_INIT;
          return ETAG_NAME;
        case STATE_SCAN_STAG_NAME:

          // dangerous call!!!!
          //if(reading && pos > 2
          //    //&& (bufEnd - pos) <= 64
          //    && pos > posSafe
          //    )
          //  shrink(pos - 2);

          seenContent = false;
          ch = less();
          posStart = pos - 2;
          ch = readName(ch);
          posEnd = pos - 1;
          ch = less();
          //if(ch != '>')
          state = STATE_SCAN_ATTR_NAME;
          //pcEnd = pcStart = 0; // to have place for attribute content
          pcEnd = pcStart = pcEnd + 1;
          return STAG_NAME;
        case STATE_SCAN_STAG_GT:
          if(ch == '>') {
            state = STATE_CONTENT_INIT;
            posStart = pos -1;
            posEnd = pos;
            return STAG_END;
          } else {
            throw XmlTokenizerException(
              "expected > for end of start tag not "+ch+getPosDesc(), getLineNumber(), getColumnNumber());
          }
        case STATE_SCAN_ATTR_NAME:
          //pcStart = pcEnd;
          pcEnd = pcStart = pcEnd + 1;
          ch = skipS(ch);
          if(ch == '/') { // [44] EmptyElemTag
            state = STATE_SCAN_STAG_GT;
            posStart = pos -1;
            posEnd = pos;
            mixInElement = false;
            return EMPTY_ELEMENT;
          } else if(ch == '>') {
            state = STATE_CONTENT_INIT;
            posStart = pos -1;
            posEnd = pos;
            return STAG_END;
          }
          posStart = pos - 1;
          ch = readName(ch);
          posEnd = pos - 1;
          ch = less();
          state = STATE_SCAN_ATTR_EQ;
          return ATTR_NAME;
        case STATE_SCAN_ATTR_EQ:
          ch = skipS(ch);
          if(ch != '=')
            throw XmlTokenizerException(
              "expected = after attribute name not "+ch+getPosDesc(), getLineNumber(), getColumnNumber());
          state = STATE_SCAN_ATTR_VALUE;
          break;
        case STATE_SCAN_ATTR_VALUE: // [10] AttValue
          ch = skipS(ch);
          if(ch != '\'' && ch != '"')
            throw XmlTokenizerException(
       "attribute value must start with double quote or apostrophe not "
              +ch+getPosDesc(), getLineNumber(), getColumnNumber());
          attrMarker = ch;
          state = STATE_SCAN_ATTR_VALUE_CONTINUE;
          break;
        case STATE_SCAN_ATTR_VALUE_CONTINUE:
          posEnd = posStart = pos - 1;
          state = STATE_SCAN_ATTR_VALUE_END;
          ; // fall through
        case STATE_SCAN_ATTR_VALUE_END:
          if(ch == attrMarker) {
            if(paramPC) {
              state = STATE_ATTR_VALUE_CONTENT;
            } else {
              state = STATE_SCAN_ATTR_NAME;
            }
            if(paramNotifyAttValue)
              return ATTR_CHARACTERS;
           } else if(ch == '&') {
            // BUG 36025: content of attr already copied to pc[pcStart...pCEnd] at (P) below, 
            //            however, when pcEnd==pcStart, it is not copied
            //if(paramPC && posEnd > posStart) {
            if(paramPC && posEnd > posStart && pcEnd==pcStart) {
              // TODO: joinPC()
              int len = posEnd - posStart;
              //System.arraycopy(buf, posStart, pc, pcEnd, len);
              memcpy(pc + pcEnd, buf + posStart, len * sizeof(SXT_CHAR));
              pcEnd += len;
            }
            state = STATE_SEEN_AMP;
            previousState = STATE_SCAN_ATTR_VALUE_CONTINUE;
            if(paramNotifyAttValue)
              return ATTR_CHARACTERS;
          } else if(ch == '<') {
            throw XmlTokenizerException(
              "attribute value can not contain "+ch+getPosDesc(), getLineNumber(), getColumnNumber());
          } else {
            posEnd = pos;
            //if(paramPC && pcStart != pcEnd)
            //  pc[pcEnd++] = ch;
            if(paramPC && pcStart != pcEnd
              || (paramPC && NORMALIZE_LINE_BREAKS && ch == '\r')   // ---- (P)
            ) {
              //XXX
              if(NORMALIZE_LINE_BREAKS && ch == '\r') 
                pc[pcEnd++] = '\n';
              else
                pc[pcEnd++] = ch;
            }            
          }
          break;
        case STATE_ATTR_VALUE_CONTENT:
          ch = less();
          // finishPC()
          parsedContent = (pcEnd != pcStart);
          state = STATE_SCAN_ATTR_NAME;
          return ATTR_CONTENT;

        // [18] - [21] CDSEct; 2.7 CDATA Sections
        case STATE_CDSECT:
          if(ch == ']')
            state = STATE_CDSECT_BRACKET;
          break;
        case STATE_CDSECT_BRACKET:
          if(ch == ']')
            state = STATE_CDSECT_BRACKET_BRACKET;
          else
            state = STATE_CDSECT;
          break;
        case STATE_CDSECT_BRACKET_BRACKET:
          if(ch == '>') {
            state = STATE_CONTENT_CONTINUED;
            posEnd = pos - 3;
            //NOTE: possible optimization: no copying
            //  for "<m:bar><![CDATA[TEST]]></m:bar>"
            // -- very diffucult to do
            if(paramPC && posEnd > posStart) { 
              int len = posEnd - posStart;
              //System.arraycopy(buf, posStart, pc, pcEnd, len);
              memcpy(pc + pcEnd, buf + posStart, len * sizeof(SXT_CHAR)); //FIXME UNICODE
              pcEnd += len;
            }
            if(paramNotifyCDSect)
              return CDSECT;
          } else {
            state = STATE_CDSECT;
          }
          break;

        // [15] Comment; 2.5 Comments
        case STATE_COMMENT:
          if(ch == '-')
            state = STATE_COMMENT_DASH;
          break;
        case STATE_COMMENT_DASH:
          if(ch == '-')
            state = STATE_COMMENT_DASH_DASH;
          else
            state = STATE_COMMENT;
          break;
        case STATE_COMMENT_DASH_DASH:
          if(ch == '>') {
            state = STATE_CONTENT_CONTINUED;
            posEnd = pos - 3;
            if(paramNotifyComment)
              return COMMENT;
          } else {
            state = STATE_COMMENT;
          }
          break;

        // [28] doctypedecl; 2.8 Prolog and Document Type Declaration
        case STATE_DOCTYPE:
          if(ch == '[')
            state = STATE_DOCTYPE_BRACKET;
          else if(ch == '>') {
            state = STATE_CONTENT_CONTINUED;
            posEnd = pos - 1;
            if(paramNotifyDoctype)
              return DOCTYPE;
          }
          break;
        case STATE_DOCTYPE_BRACKET:
          if(ch == ']')
            state = STATE_DOCTYPE_BRACKET_BRACKET;
          break;
        case STATE_DOCTYPE_BRACKET_BRACKET:
          ch = skipS(ch);
          if(ch == '>') {
            state = STATE_CONTENT_CONTINUED;
            posEnd = pos - 1;
            if(paramNotifyDoctype)
              return DOCTYPE;
          } else {
            throw XmlTokenizerException("expected > for DOCTYPE end not "
              +ch+getPosDesc(), getLineNumber(), getColumnNumber());
          }
          break;

        // [16]-[17] PI; 2.6 Processing Instructions
        case STATE_PI:  //TODO: enforce "XML" as reserved prefix
          if(ch == '?')
            state = STATE_PI_END;
          break;
        case STATE_PI_END:
          if(ch == '>') {
            state = STATE_CONTENT_CONTINUED;
            posEnd = pos - 2;
            if(paramNotifyPI)
              return PI;
          }
          break;
        default:
          throw XmlTokenizerException("invalid internal state "
            +state+getPosDesc(), getLineNumber(), getColumnNumber());
        }
    }
  }


    const SXT_STRING getPosDesc() const {
      //char msg[100];
      //sprintf(msg, " at line %d and column %d ", posRow, (posCol-1));
      //return string(msg); //FIXME
      //ostringstream os;
      ostringstream os;
      os << " at line " << posRow << " and column " << (posCol-1) << " ";
      return os.str();
    }

  
    int getLineNumber() const { return posRow; }
    int getColumnNumber() const { return posCol-1; }


  private:
  // ========= input buffer management

  /**
   * Get next available character from input.
   * If it is last character set internal flag reachedEnd.
   * If there are no more characters throw XmlTokenizerException.
   */
    SXT_CHAR more() throw ( XmlTokenizerException ) {
      if(backtracking) {
        backtracking = false;
        //++pos;
        //++posCol;
        return prevCh;  
      }
      if(pos == bufEnd - 1)
        reachedEnd = true;
      if(pos >= bufEnd)
        throw XmlTokenizerException("no more data available");
      assert(pos < bufEnd);
      SXT_CHAR ch = buf[pos++];
      // update (row,colum) position so far - new lines:
      //  1) "\r\n","\r\n"...  2) "\n","\n",...  3) "\r","\r","\r"
      if(ch == '\n' || ch == '\r') {
         if ( prevCh != '\r' || ch != '\n' ) {
           posCol = 2; // always one char ahead
           ++posRow;
         }
         //cerr << "ALEK NEW LINE ch =" << ch << endl;
      } else {
        ++posCol;
      }
      prevPrevCh = prevCh;
      return prevCh = ch;
    }

    SXT_CHAR less() {
      //NOTE: trick - we are backtracing one characker....
      //--pos;
      //--posCol;
      backtracking = true;
      //return buf[pos - 1];
      return prevPrevCh;
    }

    /*
    void shrink(int posCut) {
      //System.err.println("shrink posCut="+posCut+" bufSize="+bufSize+" pos="+pos);
      //System.arraycopy(buf, posCut, buf, 0, bufEnd - posCut);
      memcpy(buf, buf + posCut, (bufEnd - posCut) * sizeof(SXT_CHAR)); //TODO: check for wchar_t
      bufEnd -= posCut;
      pos -= posCut;
      posStart -= posCut;
      posEnd -= posCut;
      posNsColon -= posCut;
    }
    */

    // ==== utility methods

   const SXT_STRING to_string(int i) const {
      //char msg[100];
      //sprintf(msg, "%d", i); //FIXME UNICODE
      //return SXT_STRING(msg);
      ostringstream os;
      os << i;
      return os.str();
    }


    // ============ utility methods


    string printable(SXT_CHAR ch) {
      if(ch == '\n') {
        return "\\n";
      } else if(ch == '\r') {
        return "\\r";
      } else if(ch == '\t') {
        return "\\t";
      }
      return string("")+ch;
    }


    /**
     * Read name from input or throw exception ([4] NameChar, [5] Name).
     */
    // TODO: make it fully complaint with XML spec
    char readName(char ch) throw (XmlTokenizerException) {
      posNsColon = -1;
      nsColonCount = 0;
      if(!(ch >= 'A' && ch <= 'Z') && !(ch >= 'a' && ch <= 'z') 
        && ch != '_' && ch != ':')
        throw XmlTokenizerException(string("expected name start not ")
          +ch+getPosDesc(), getLineNumber(), getColumnNumber());
      do {
        ch = more();
        if(ch == ':') {
          posNsColon = pos - 1;
          ++nsColonCount;
        }
      } while((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
              || (ch >= '0' && ch <= '9')
              || ch == '.' || ch == '-'
              || ch == '_' || ch == ':');
      return ch;
    }

    /**
     * Determine if ch is whitespace ([3] S)
     */
    bool isS(char ch) {
      return (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r');
    }


    char skipS(char ch) throw (XmlTokenizerException) {
      while(ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r')
        ch = more();
      return ch;
    }

    char readS(char ch) throw (XmlTokenizerException) {
      if(!isS(ch))
        throw XmlTokenizerException(string("expected white space not ")
          +ch+getPosDesc(), getLineNumber(), getColumnNumber());
      while(ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r')
        ch = more();
      return ch;
    }

    friend ostream& operator<<(ostream& output, 
     const XmlTokenizer& tokenizer);


  private:

    void init() {
      // C++ does not initialize memeber fields (not like Java....)

      paramNotifyCharacters = false;
      paramNotifyComment = false;
      paramNotifyCDSect = false;
      paramNotifyDoctype = false;
      paramNotifyPI = false;
      paramNotifyCharRef = false;
      paramNotifyEntityRef = false;
      paramNotifyAttValue = false;

      buf = NULL; //new SXT_CHAR[BUF_SIZE];
      pos = posStart = posEnd = posNsColon = 0;
      //pc = NULL;
      pcStart = pcEnd = 0;
      seenContent = false;

      paramPC = true;
      mixInElement = false;
      paramNoMixContent = false;
      pc = NULL;
      pcSize = 0;

      readChunkSize = 1024;
      loadFactor = 0.99f;
      posSafe = 0; //(int)(loadFactor * BUF_SIZE);
      bufEnd = bufSize = 0; //BUF_SIZE;
      posCol = posRow = 0;

      attrMarker = 0;
      backtracking = false;
      charRefValue = 0;
      charRefHex = false;
      reachedEnd = false;
      state = previousState = -1;
      //if(paramPC) {
      //  pc = new SXT_CHAR[BUF_SIZE];
      //  pcSize = BUF_SIZE;
      //}
      reset();
    }

    void reset() {  
      seenContent = false;
      mixInElement = false;
      bufEnd = 0;
      posEnd = posStart = pos = 0;
      posNsColon = -1;
      state = STATE_INIT;
      prevPrevCh = prevCh = '\0';
      posCol = posRow = 1;
      reachedEnd = false;
      pcEnd = pcStart = 0;
      previousState = -1;
      bufSize = 0;
      pcSize = 0;
      parsedContent =  false;
      posSafe = 0;
      backtracking = false;
    }

    void done() {
      reset();
      if(buf != NULL) {
        delete [] buf;
        buf = NULL;
      }
      if(pc != NULL) {
        delete [] pc;
        pc = NULL;
      }
    }


    enum {
     NORMALIZE_LINE_BREAKS = 1
    };
  

    /** Parsed Content reporting */
    bool paramPC;
    bool mixInElement;

    /** Allow mixed content ? */
    bool paramNoMixContent;

    int pcSize;


    //static const int BUF_SIZE = 12 * 1024;
    int readChunkSize;
    float loadFactor;
    int posSafe;

    int bufEnd;
    int bufSize;


    int posCol;
    int posRow;
    SXT_CHAR prevCh;
    SXT_CHAR prevPrevCh;

    SXT_CHAR attrMarker;
    bool backtracking;
    SXT_CHAR charRefValue;
    bool charRefHex;
    bool reachedEnd;
    int previousState;
    int state;

    // ==== internal state
      enum {
      STATE_INIT                    = 1,
      STATE_FINISH                  = 6,
      STATE_FINISHED                = 7,
      STATE_CONTENT_INIT            = 10,
      STATE_CONTENT_CONTINUED       = 11,
      STATE_CONTENT                 = 12,
      STATE_SEEN_LT                 = 13,
      STATE_SEEN_LT_BANG            = 14,
      STATE_CDSECT                  = 30,
      STATE_CDSECT_BRACKET          = 31,
      STATE_CDSECT_BRACKET_BRACKET  = 32,
      STATE_COMMENT                 = 40,
      STATE_COMMENT_DASH            = 41,
      STATE_COMMENT_DASH_DASH       = 42,
      STATE_DOCTYPE                 = 50,
      STATE_DOCTYPE_BRACKET         = 51,
      STATE_DOCTYPE_BRACKET_BRACKET = 52,
      STATE_PI                      = 60,
      STATE_PI_END                  = 61,
      STATE_SEEN_AMP                = 70,
      STATE_ENTITY_REF              = 71,
      STATE_CHAR_REF                = 75,
      STATE_CHAR_REF_DIGITS         = 76,
      STATE_SCAN_ETAG_NAME          = 110,
      STATE_SCAN_STAG_NAME          = 120,
      STATE_SCAN_STAG_GT            = 121,
      STATE_SCAN_ATTR_NAME          = 122,
      STATE_SCAN_ATTR_EQ            = 123,
      STATE_SCAN_ATTR_VALUE         = 124,
      STATE_SCAN_ATTR_VALUE_CONTINUE= 125,
      STATE_SCAN_ATTR_VALUE_END     = 126,
      STATE_ATTR_VALUE_CONTENT      = 127
      };
  };


inline ostream& operator<<(ostream& output, 
  const XmlTokenizer& tokenizer) 
{
    SXT_STRING ss = tokenizer.to_string(tokenizer.state);
    SXT_STRING s = "XmlTokenizer: current state: "+ss;
    output << s << endl;
    return output;
}


} // namespace

#pragma warning(pop)

#endif // SXT_XML_TOKENIZER_H_

