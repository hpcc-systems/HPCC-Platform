#!/usr/bin/env python3
"""
Jirabot - Process GitHub Pull Requests and update JIRA issues
This script is called from the secure jirabot-process workflow
"""
import os
import re
import sys
import json
import subprocess
from email.utils import parseaddr
from atlassian.jira import Jira


def sanitize_input(input_str: str, input_type: str) -> str:
    """Sanitize input based on type"""
    if input_type.lower() == 'email':
        # Return the email address only, returns '' if not valid or found
        return parseaddr(input_str)[1]
    else:
        return ''


def update_issue(jira, issue, pr_author: str, property_map: dict, pull_url: str) -> str:
    """Update JIRA issue with PR information"""
    result = ''

    issue_name = issue['key']
    issue_fields = issue['fields']

    # Need to update user first in case we are starting from Unresourced
    if pr_author:
        assignee = issue_fields['assignee']
        if assignee is None:
            assignee_id = ''
            assignee_email = ''
        else:
            assignee_id = assignee['accountId']
            assignee_email = assignee["emailAddress"]

        assignee_email = sanitize_input(assignee_email, 'email')

        pr_author_id = pr_author["accountId"]
        pr_author_email = pr_author["emailAddress"]
        pr_author_email = sanitize_input(pr_author_email, 'email')

        if assignee_id is None or assignee_id == '':
            jira.assign_issue(issue_name, pr_author_id)
            result += 'Assigning user: ' + pr_author_email + '\n'
        elif assignee_id != pr_author_id:
            result += 'Changing assignee from: ' + assignee_email + ' to: ' + pr_author_email + '\n'
            jira.assign_issue(issue_name, pr_author_id)

    transition_flow = ['Merge Pending']
    for desired_status in transition_flow:
        try:
            transition_id = jira.get_transition_id_to_status_name(issue_name, desired_status)
            jira.set_issue_status_by_transition_id(issue_name, transition_id)
            result += 'Workflow Transition To: ' + desired_status + '\n'
        except Exception as error:
            transitions = jira.get_issue_transitions(issue_name)
            result += 'Error: Transitioning to: "' + desired_status + '" failed with: "' + str(error) + '" Valid transitions=' + str(transitions) + '\n'

    pr_field_name = property_map.get('pullRequestFieldName', 'customfield_10010')

    if pr_field_name in issue_fields:
        current_pr = issue_fields[pr_field_name]
    else:
        print('Error: Unable to find pull request field with field name: ' + pr_field_name)
        current_pr = None

    if current_pr is None:
        jira.update_issue_field(issue_name, {pr_field_name: pull_url})
        result += 'Updated PR\n'
    elif current_pr is not None and current_pr != pull_url:
        result += 'Additional PR: ' + pull_url + '\n'

    return result


def main():
    """Main function"""
    # Get environment variables
    jirabot_user = os.environ['JIRABOT_USERNAME']
    jirabot_pass = os.environ['JIRABOT_PASSWORD']
    jira_url = os.environ['JIRA_URL']
    pr_number = os.environ['PR_NUMBER']
    title = os.environ['PR_TITLE']
    pr_author = os.environ['PR_AUTHOR']
    pull_url = os.environ['PR_URL']
    github_token = os.environ['GITHUB_TOKEN']
    comments_url = os.environ['PR_COMMENTS_URL']

    result = ''
    issue_match = re.search("(HPCC|HH|IDE|EPE|ML|HPCC4J|JAPI)-[0-9]+", title)
    
    if issue_match:
        issue_name = issue_match.group()

        user_dict = json.loads(os.environ['GHUB_JIRA_USER_MAP'])
        if not isinstance(user_dict, dict):
            user_dict = {}

        if pr_author in user_dict:
            pr_author = user_dict.get(pr_author)
            print('Mapped Github user to Jira user: ' + pr_author)

        jira = Jira(url=jira_url, username=jirabot_user, password=jirabot_pass, cloud=True)

        jira_user = None
        user_search_results = jira.user_find_by_user_string(query=pr_author)
        if user_search_results and len(user_search_results) > 0:
            jira_user = user_search_results[0]
        else:
            print('Error: Unable to map GitHub user to Jira user, continuing without assigning')

        if not jira.issue_exists(issue_name):
            sys.exit('Error: Unable to find Jira issue: ' + issue_name)
        else:
            issue = jira.issue(issue_name)

        result = 'Jirabot Action Result:\n'

        jira_issue_property_map = json.loads(os.environ['JIRA_ISSUE_PROPERTY_MAP'])
        if not isinstance(jira_issue_property_map, dict):
            print('Error: JIRA_ISSUE_PROPERTY_MAP is not a valid JSON object, ignoring.')
            jira_issue_property_map = {}

        result += update_issue(jira, issue, jira_user, jira_issue_property_map, pull_url)
        jira.issue_add_comment(issue_name, result)

        result = 'Jira Issue: ' + jira_url + '/browse/' + issue_name + '\n\n' + result

        # Escape the result for JSON
        result = json.dumps(result)

        subprocess.run(['curl', '-X', 'POST', comments_url, '-H', 'Content-Type: application/json', '-H', f'Authorization: token {github_token}', '--data', f'{{ "body": {result} }}'], check=True)
    else:
        print('Unable to find Jira issue name in title')

    print(result)


if __name__ == "__main__":
    main()
