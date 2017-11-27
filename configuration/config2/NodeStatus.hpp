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

#ifndef _CONFIG2_NODESTATUS_HPP_
#define _CONFIG2_NODESTATUS_HPP_

#include <map>

class NodeStatus
{
	public:
		
		enum nodeStatus
		{
			ok,
			warning,
			error,
			fatal
		};

		NodeStatus() : m_highestStatus(ok) { }
		~NodeStatus() {}
		void addStatus(nodeStatus status, const std::string &msg);
		nodeStatus getStatus() const { return m_highestStatus; }
		std::string getStatusString(nodeStatus status) const;
		void clearStatus() { m_highestStatus = ok;  m_messages.clear(); }


	private:

		nodeStatus m_highestStatus;
		std::multimap<nodeStatus, std::string> m_messages;

};

#endif