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

#ifndef __MIME_HPP_
#define __MIME_HPP_

class CMimeBodyPart : public CInterface, implements IInterface
{
private:
    StringAttr m_content_type;
    StringAttr m_encoding;
    StringAttr m_cid;
    StringAttr m_location;
    StringAttr m_content_disposition;
    StringBuffer m_content;

public:
    IMPLEMENT_IINTERFACE;

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
    IArrayOf<CMimeBodyPart> m_parts;

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
    void checkEndOfFile(MemoryBuffer& fileContent);
    void readUploadFile(MemoryBuffer& fileContent, StringBuffer& fileName);

    void serialize(StringBuffer& contenttype, StringBuffer & buffer);
    void unserialize(const char* contenttype, int text_length, const char* text);
};

#endif

