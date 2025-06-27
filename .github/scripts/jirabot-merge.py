#!/usr/bin/env python3
"""
Jirabot Merge - Process merged GitHub Pull Requests and resolve JIRA issues
This script is called from the secure jirabot-process-merge workflow
"""
import os
import re
import sys
import json
import shlex
import subprocess
import requests
from atlassian.jira import Jira


def sanitize_input(input_str: str, input_type: str) -> str:
    """Sanitize input based on type"""
    if input_type.lower() == 'text':
        # Remove potentially dangerous characters
        import re
        # Allow alphanumeric, spaces, hyphens, underscores, and basic punctuation
        sanitized = re.sub(r'[^\w\s\-._@#()[\]{},;:!?+=*/\\&%$]', '', input_str)
        return sanitized.strip()
    elif input_type.lower() == 'branch_name':
        # Branch names should only contain safe characters
        sanitized = re.sub(r'[^a-zA-Z0-9\-_./]', '', input_str)
        return sanitized.strip()
    else:
        return input_str


def escape_regex_chars(text: str) -> str:
    """Escape regex metacharacters in text"""
    return re.escape(text)


def extract_version(version_str):
    """Extract version components from version string"""
    parts = version_str.split('.')
    if len(parts) != 3:
        print('Invalid version: ' + version_str)
        sys.exit(1)
    if parts[2].lower() == 'x':
        parts[2] = '0'

    major, minor, point = map(int, parts)
    return [major, minor, point]


def get_tag_version_for_cmd(pattern):
    """Get version from git tag command"""
    version_pattern = re.compile(r".*([0-9]+\.[0-9]+\.[0-9]+).*")

    # Get latest release version - use safe subprocess call
    try:
        # Use safe subprocess call with proper shell escaping
        cmd = ['git', 'tag', '--list', '--sort=-v:refname']
        git_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = git_process.communicate()
        git_tag_process_status = git_process.wait()

        if git_tag_process_status != 0:
            print('Unable to retrieve latest git tag.')
            sys.exit(1)

        # Filter the output using Python instead of shell grep
        output_str = output.decode('utf-8')
        pattern_re = re.compile(pattern)
        for line in output_str.split('\n'):
            if pattern_re.match(line.strip()):
                latest_git_tag = line.strip()
                break
        else:
            print('No matching git tag found.')
            sys.exit(1)

    except Exception as e:
        print(f'Error executing git command: {e}')
        sys.exit(1)

    version_match = version_pattern.match(latest_git_tag)
    if version_match:
        return extract_version(version_match.group(1))
    else:
        print('Unable to extract version from git tag.')
        sys.exit(2)


def build_version_string(version):
    """Build version string from version array"""
    major, minor, point = map(int, version)
    return f"{major}.{minor}.{point}"


def create_release_tag_pattern(project_config, major=None, minor=None, point=None):
    """Create regex pattern for release tags"""
    release_tag_prefix = project_config.get('tagPrefix')
    release_tag_postfix = project_config.get('tagPostfix')

    if release_tag_prefix is None or release_tag_postfix is None:
        print('Error: PROJECT_CONFIG is missing required fields: tagPrefix and/or tagPostfix')
        sys.exit(1)

    release_tag_pattern = release_tag_prefix
    if major is not None:
        release_tag_pattern += str(major) + '\\.'
    else:
        release_tag_pattern += '[0-9]+\\.'

    if minor is not None:
        release_tag_pattern += str(minor) + '\\.'
    else:
        release_tag_pattern += '[0-9]+\\.'

    if point is not None:
        release_tag_pattern += str(point) + '(-[0-9]+)?'
    else:
        release_tag_pattern += '[0-9]+(-[0-9]+)?'

    release_tag_pattern += release_tag_postfix + '$'

    return release_tag_pattern


def get_latest_sem_ver(project_config, major=None, minor=None, point=None):
    """Get latest semantic version matching criteria"""
    pattern = create_release_tag_pattern(project_config, major, minor, point)
    return get_tag_version_for_cmd(pattern)


def generate_fix_version_list(jira, project_config, project_name, branch_name):
    """Generate list of fix versions for the issue"""
    latest_version = get_latest_sem_ver(project_config)

    # If we are merging into master we assume it is going into the next minor release
    fix_versions = []
    if branch_name == "master":
        fix_versions = [build_version_string([latest_version[0], latest_version[1] + 2, 0])]
    else:
        # Extract candidate branch major / minor version
        candidate_branch_pattern = re.compile(r"candidate-([0-9]+\.[0-9]+\.([0-9]+|x)).*")
        branch_version_match = candidate_branch_pattern.match(branch_name)
        branch_version = extract_version(branch_version_match.group(1))

        # Get latest release in branch
        latest_branch_ver = get_latest_sem_ver(project_config, branch_version[0], branch_version[1])

        cur_major = branch_version[0]
        latest_major = latest_version[0]
        while cur_major <= latest_major:
            latest_version_in_major = get_latest_sem_ver(project_config, cur_major)

            cur_minor = 0
            if cur_major == branch_version[0]:
                cur_minor = branch_version[1]

            latest_minor = latest_version_in_major[1]

            while cur_minor <= latest_minor:
                latest_point_in_minor = get_latest_sem_ver(project_config, cur_major, cur_minor)
                fix_versions.append(build_version_string([latest_point_in_minor[0], latest_point_in_minor[1], latest_point_in_minor[2] + 2]))
                cur_minor += 2
            cur_major += 1

    for fix_version in fix_versions:
        try:
            already_has_fix_version = False
            versions = jira.get_project_versions(project_name)
            for v in versions:
                if v['name'] == fix_version:
                    already_has_fix_version = True

            if not already_has_fix_version:
                project = jira.get_project(project_name)
                project_id = project['id']
                jira.add_version(project_name, project_id, fix_version)
        except Exception as error:
            print('Error: Unable to add fix version: ' + fix_version + ' with: ' + str(error))
            sys.exit(1)

    return fix_versions


def resolve_issue(jira, project_name, issue, fix_versions) -> str:
    """Resolve JIRA issue with fix versions"""
    result = ''

    versions_to_add = []

    issue_name = issue['key']
    issue_fields = issue['fields']

    for added_version in fix_versions:
        already_has_fix_version = False
        for v in issue_fields['fixVersions']:
            if v['name'] == added_version:
                already_has_fix_version = True
                break
        if not already_has_fix_version:
            versions_to_add.append(added_version)

    versions = jira.get_project_versions(project_name)
    updated_version_list = []
    for v in issue_fields['fixVersions']:
        updated_version_list.append({'id': v['id']})

    for fix_version_name in versions_to_add:
        fix_version = None
        for v in versions:
            if v['name'] == fix_version_name:
                fix_version = v
                break

        if fix_version:
            updated_version_list.append({'id': fix_version['id']})
            result += "Added fix version: " + fix_version_name + "\n"
        else:
            result += "Error: Unable to find fix version: " + fix_version_name + "\n"

    if len(versions_to_add) > 0:
        try:
            jira.update_issue_field(issue_name, {'fixVersions': updated_version_list})
        except Exception as error:
            result += 'Error: Updating fix versions failed with: "' + str(error) + '\n'
    else:
        result += "Fix versions already added.\n"

    status_name = str(issue_fields['status']['name'])
    if status_name != 'Resolved':
        try:
            transition_id = jira.get_transition_id_to_status_name(issue_name, 'Resolved')
            jira.set_issue_status_by_transition_id(issue_name, transition_id)
            result += "Workflow Transition: 'Resolve issue'\n"
        except Exception as error:
            result += 'Error: Transitioning to: "Resolved" failed with: "' + str(error) + '\n'

    return result


def main():
    """Main function"""
    # Get environment variables
    jirabot_user = os.environ['JIRABOT_USERNAME']
    jirabot_pass = os.environ['JIRABOT_PASSWORD']
    jira_url = os.environ['JIRA_URL']

    pr_number = os.environ['PR_NUMBER']
    title = sanitize_input(os.environ['PR_TITLE'], 'text')
    user = sanitize_input(os.environ['PR_AUTHOR'], 'text')
    pull_url = os.environ['PR_URL']
    github_token = os.environ['GITHUB_TOKEN']
    branch_name = sanitize_input(os.environ['BRANCH_NAME'], 'branch_name')
    comments_url = os.environ['PR_COMMENTS_URL']

    project_config = json.loads(os.environ['PROJECT_CONFIG'])
    if not isinstance(project_config, dict):
        print('Error: PROJECT_CONFIG is not a valid JSON object, aborting.')
        sys.exit(1)

    if 'tagPrefix' not in project_config or 'tagPostfix' not in project_config:
        print('Error: PROJECT_CONFIG is missing required fields: tagPrefix and/or tagPostfix')
        sys.exit(1)

    project_prefixes = project_config.get('projectPrefixes')
    if project_prefixes is None:
        print('Error: PROJECT_CONFIG is missing required field: projectPrefixes')
        sys.exit(1)

    project_list_regex = '|'.join([escape_regex_chars(prefix) for prefix in project_prefixes])

    result = ''
    issue_match = re.search("(" + project_list_regex + ")-[0-9]+", title, re.IGNORECASE)
    if issue_match:
        project_name = issue_match.group(1)
        issue_name = issue_match.group()

        jira = Jira(url=jira_url, username=jirabot_user, password=jirabot_pass, cloud=True)

        if not jira.issue_exists(issue_name):
            sys.exit('Error: Unable to find Jira issue: ' + issue_name)
        else:
            issue = jira.issue(issue_name)

        result = 'Jirabot Action Result:\n'

        fix_versions = generate_fix_version_list(jira, project_config, project_name, branch_name)
        result += resolve_issue(jira, project_name, issue, fix_versions)
        jira.issue_add_comment(issue_name, result)

        # Escape the result for JSON
        result = json.dumps(result)

        # Use requests instead of subprocess to avoid token exposure
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'token {github_token}'
        }
        payload = {'body': result}
        response = requests.post(comments_url, headers=headers, json=payload)
        response.raise_for_status()
    else:
        print('Unable to find Jira issue name in title')

    print(result)


if __name__ == "__main__":
    main()
