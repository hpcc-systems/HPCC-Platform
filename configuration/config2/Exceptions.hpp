/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#ifndef _CONFIG2_CONFIGEXCEPTIONS_HPP_
#define _CONFIG2_CONFIGEXCEPTIONS_HPP_

#include <exception>
#include <string>
#include <vector>

class ParseException : public std::exception
{
    public:

        ParseException(const std::string &reason) : m_reason(reason) { };
        ParseException(const char *reason) : m_reason(reason) { };
        ParseException() { };

        void addFilename(const std::string &filename) { m_filenames.push_back(filename); }

        virtual void setMessage(const char *msg) { m_reason = msg; }
        virtual void setMessage(const std::string &msg) { m_reason = msg; }

        virtual const char *what() const throw()
        {
            std::string &msg = const_cast<std::string &>(m_reason);

            if (m_filenames.size())
            {
                bool first = true;
                msg += " While parsing: ";
                for (auto it = m_filenames.begin(); it != m_filenames.end(); ++it)
                {
                    if (!first)
                        msg += "-->";
                    msg += (*it);
                    first = false;
                }
            }

            return m_reason.c_str();
        }


    private:

        std::string m_reason;
        std::vector<std::string> m_filenames;

};

#endif