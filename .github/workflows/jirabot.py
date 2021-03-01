#!/usr/bin/python
import sys
import re
from jira.client import JIRA

def update_jira(title, pull_url, user):
    options = {
        'server': 'https://track.hpccsystems.com'
    }
    issuem = re.search("(HPCC|HH|IDE|EPE|ML|ODBC)-[0-9]+", title)
    if (action =='opened' or action=='reopened') and issuem:
        issue = issuem.group()
    if user == 'dehilsterlexis':
        user = 'dehilster'
    if user == 'kunalaswani':
        user = 'kunal.aswani'
    if user == 'timothyklemm':
        user = 'klemti01'
    if user == 'jpmcmu':
        user = 'mcmuja01'
    if user == 'asselitx':
        user = 'terrenceasselin'
    jira = JIRA(options=options, basic_auth=('hpcc-jirabot', 'hpcc-jirabot1'))
    issue = jira.issue(issue_name)
    print (issue.fields.assignee)
    status = 'https://track.hpccsystems.com/browse/' + issue_name + '\n'
    if False and issue.fields.status.name != 'Active' and issue.fields.status.name != 'Open' and issue.fields.status.name != 'New' and issue.fields.status.name != 'Discussing' and issue.fields.status.name != 'Awaiting Information':
        status += 'Jira not updated (state was not active or new)'
    elif issue.fields.customfield_10010 != None:
        status += 'Jira not updated (pull request already registered)'
    elif issue.fields.assignee is not None and issue.fields.assignee.name.lower() != user.lower():
        status += 'Jira not updated (user does not match)'
    else:
        if issue.fields.assignee is None:
            jira.assign_issue(issue, user)
        issue.update(fields={'customfield_10010': pull_url})
        transitions = jira.transitions(issue)
        jira.transition_issue(issue, '291')   # Attach Pull Request
        status += 'Jira updated'
    return status

if __name__ == "__main__":
  status = update_jira(sys.argv[1], sys.argv[2], sys.argv[3])
  print(status)
