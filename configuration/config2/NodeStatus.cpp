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


#include "NodeStatus.hpp"


void NodeStatus::addStatus(nodeStatus status, const std::string &msg)
{
	m_messages.insert({ status, msg });
	if (status > m_highestStatus)
		m_highestStatus = status;
}


std::string NodeStatus::getStatusString(nodeStatus status) const
{
	std::string result = "Not found";
	switch (status)
	{
		case ok:      result = "Ok";       break;
		case warning: result = "Warning";  break;
		case error:   result = "Error";    break;
		case fatal:   result = "Fatal";    break;
	}
	return result;
}