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

#ifndef RMTSMTP_HPP
#define RMTSMTP_HPP

#include "jbuff.hpp"
#include "jstring.hpp"

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

extern REMOTE_API void sendEmail( const char * to, const char * cc, const char * bcc, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);
extern REMOTE_API void sendEmail( const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);

extern REMOTE_API void sendEmailAttachText(const char * to, const char * cc, const char * bcc, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);
extern REMOTE_API void sendEmailAttachText(const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);

extern REMOTE_API void sendEmailAttachData(const char * to, const char * cc, const char * bcc, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);
extern REMOTE_API void sendEmailAttachData(const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, StringArray *warnings=NULL, bool highPriority=false);


#endif
