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

#ifndef RMTSMTP_HPP
#define RMTSMTP_HPP

#include "jbuff.hpp"
#include "jstring.hpp"

#ifdef REMOTE_EXPORTS
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
#endif

extern REMOTE_API void sendEmail( const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, StringArray *warnings=NULL);
extern REMOTE_API void sendEmailAttachText(const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL);
extern REMOTE_API void sendEmailAttachData(const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL);


#endif
