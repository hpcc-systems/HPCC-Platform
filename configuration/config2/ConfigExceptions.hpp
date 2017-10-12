/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

class ParseException : public std::exception
{
    public:

        ParseException(const std::string &reason) : m_reason(reason) { };
        ParseException(const char *reason) : m_reason(reason) { };

        virtual const char *what() const throw()
        {
            return m_reason.c_str();
        }

    
    private:

        std::string m_reason;

};


class ValueException : public std::exception
{
    public:

        ValueException(const std::string &reason) : m_reason(reason) { };
        ValueException(const char *reason) : m_reason(reason) { };

        virtual const char *what() const throw()
        {
            return m_reason.c_str();
        }

    
    private:

        std::string m_reason;

};


class ConfigException : public std::exception
{
public:

	ConfigException(const std::string &reason) : m_reason(reason) { };
	ConfigException(const char *reason) : m_reason(reason) { };

	virtual const char *what() const throw()
	{
		return m_reason.c_str();
	}


private:

	std::string m_reason;

};

#endif