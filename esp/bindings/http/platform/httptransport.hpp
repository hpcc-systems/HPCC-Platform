/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef _HTTPTRANSPORT_HPP__
#define _HTTPTRANSPORT_HPP__

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

#define HTTP_VERSION                    "HTTP/1.1"

//TODO: fill in more status codes
#define HTTP_STATUS_OK                      "200 OK"
#define HTTP_STATUS_NO_CONTENT              "204 No Content"
#define HTTP_STATUS_MOVED_PERMANENTLY       "301 Moved Permanently"
#define HTTP_STATUS_REDIRECT                "302 Found"
#define HTTP_STATUS_REDIRECT_POST           "303 See Other"
#define HTTP_STATUS_NOT_MODIFIED            "304 Not Modified"
#define HTTP_STATUS_BAD_REQUEST             "400 Bad Request"
#define HTTP_STATUS_UNAUTHORIZED            "401 Unauthorized"
#define HTTP_STATUS_FORBIDDEN               "403 Forbidden"
#define HTTP_STATUS_NOT_FOUND               "404 Not Found"
#define HTTP_STATUS_NOT_ALLOWED             "405 Method Not Allowed"
#define HTTP_STATUS_INTERNAL_SERVER_ERROR   "500 Internal Server Error"
#define HTTP_STATUS_NOT_IMPLEMENTED         "501 Not Implemented"

#define HTTP_TYPE_TEXT_HTML                     "text/html"
#define HTTP_TYPE_TEXT_PLAIN                    "text/plain"
#define HTTP_TYPE_TEXT_XML                      "text/xml"
#define HTTP_TYPE_APPLICATION_XML               "application/xml"
#define HTTP_TYPE_IMAGE_JPEG                    "image/jpeg"
#define HTTP_TYPE_OCTET_STREAM                  "application/octet-stream"
#define HTTP_TYPE_SOAP                          "application/soap"
#define HTTP_TYPE_MULTIPART_RELATED             "Multipart/Related"
#define HTTP_TYPE_MULTIPART_FORMDATA            "multipart/form-data"
#define HTTP_TYPE_FORM_ENCODED                  "application/x-www-form-urlencoded"
#define HTTP_TYPE_SVG_XML                       "image/svg+xml"
#define HTTP_TYPE_JAVASCRIPT                    "application/x-javascript"

#define HTTP_TYPE_TEXT_HTML_UTF8                "text/html; charset=UTF-8"
#define HTTP_TYPE_TEXT_PLAIN_UTF8               "text/plain; charset=UTF-8"
#define HTTP_TYPE_TEXT_XML_UTF8                 "text/xml; charset=UTF-8"
#define HTTP_TYPE_APPLICATION_XML_UTF8          "application/xml; charset=UTF-8"
#define HTTP_TYPE_SOAP_UTF8                     "application/soap; charset=UTF-8"


#define HTTP_STATUS_OK_CODE                 200
#define HTTP_STATUS_NO_CONTENT_CODE         204
#define HTTP_STATUS_MOVED_PERMANENTLY_CODE  301
#define HTTP_STATUS_REDIRECT_CODE           302
#define HTTP_STATUS_REDIRECT_POST_CODE      303
#define HTTP_STATUS_NOT_MODIFIED_CODE       304
#define HTTP_STATUS_BAD_REQUEST_CODE        400
#define HTTP_STATUS_UNAUTHORIZED_CODE       401
#define HTTP_STATUS_PAYMENT_REQUIRED_CODE   402
#define HTTP_STATUS_FORBIDDEN_CODE          403
#define HTTP_STATUS_NOT_FOUND_CODE          404
#define HTTP_STATUS_NOT_ALLOWED_CODE        405
#define HTTP_STATUS_NOT_ACCEPTABLE_CODE     406
#define HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED_CODE  407
#define HTTP_STATUS_REQUEST_TIMEOUT_CODE    408
#define HTTP_STATUS_CONFLICT_CODE           409
#define HTTP_STATUS_GONE_CODE               410
#define HTTP_STATUS_LENGTH_REQUIRED_CODE    411
#define HTTP_STATUS_PRECONDITION_FAILED_CODE        412
#define HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE_CODE   413
#define HTTP_STATUS_REQUEST_URI_TOO_LARGE_CODE      414
#define HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE_CODE     415
#define HTTP_STATUS_REQUESTED_RANGE_NOT_SATISFIABLE_CODE    416
#define HTTP_STATUS_EXPECTATION_FAILED_CODE 417
#define HTTP_STATUS_INTERNAL_SERVER_ERROR_CODE      500
#define HTTP_STATUS_NOT_IMPLEMENTED_CODE            501
#define HTTP_STATUS_BAD_GATEWAY_CODE                502
#define HTTP_STATUS_SERVICE_UNAVAILABLE_CODE        503
#define HTTP_STATUS_GATEWAY_TIMEOUT_CODE            504
#define HTTP_STATUS_HTTP_VERSION_NOT_SUPPORTED_CODE 505

#ifdef _DEBUG
//#define DEBUG_HTTP_
#endif

bool httpContentFromFile(const char *filepath, StringBuffer &mimetype, MemoryBuffer &fileContents);
bool xmlContentFromFile(const char *filepath, const char *stylesheet, StringBuffer &fileContents);


#endif
