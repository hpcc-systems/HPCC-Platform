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

#ifndef __MIME_HPP_
#define __MIME_HPP_

class CMimeBodyPart : public CInterface
{
private:
    StringAttr m_content_type;
    StringAttr m_encoding;
    StringAttr m_cid;
    StringAttr m_location;
    StringAttr m_content_disposition;
    StringBuffer m_content;

public:
    CMimeBodyPart(const char* content_type, const char* encoding, const char* cid, const char* location, StringBuffer* content, const char* content_disposition = NULL);
    virtual ~CMimeBodyPart();
    const char* getContentType();
    const char* getContentDisposition();
    const char* getEncoding();
    const char* getCid();
    const char* getLocation();
    StringBuffer& getContent(StringBuffer& content);
    void setContent(int len, const char* buffer);

    void serialize(StringBuffer& buffer);
};

class CMimeMultiPart : public CInterface, implements IInterface
{
private:
    StringAttr m_mime_version;
    StringAttr m_content_type;
    StringAttr m_boundary;
    StringAttr m_type;
    StringAttr m_start;
    CIArrayOf<CMimeBodyPart> m_parts;

public:
    IMPLEMENT_IINTERFACE;

    CMimeMultiPart(const char* mime_version, const char* content_type, const char* boundary, const char* type, const char* start);
    virtual ~CMimeMultiPart();

    const char* getContentType();

    void setRootPart(CMimeBodyPart* part);
    CMimeBodyPart* getRootPart();
    CMimeBodyPart* queryRootPart();

    void addBodyPart(CMimeBodyPart* part);
    CMimeBodyPart* getBodyPart(const char* cid);
    CMimeBodyPart* queryBodyPart(const char* cid);
    CMimeBodyPart* getBodyPart(unsigned int seq);
    CMimeBodyPart* queryBodyPart(unsigned int seq);

    int getBodyCount();
    void setContentType(const char* content_type);

    void parseContentType(const char* contenttype);
    bool separateMultiParts(MemoryBuffer& firstPart, MemoryBuffer& otherParts, __int64 contentNotRead);
    void readUploadFileName(MemoryBuffer& fileContent, StringBuffer& fileName);

    void serialize(StringBuffer& contenttype, StringBuffer & buffer);
    void unserialize(const char* contenttype, __int64 text_length, const char* text);
};

#endif

