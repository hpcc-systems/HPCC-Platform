#!/usr/bin/env python3
"""
Rollback RC Tags and Commits

This script safely rolls back RC tags and commits that were accidentally created
when running gorc.sh with the wrong version (e.g., gorc.sh 9.14.x instead of 9.14.52).

WHAT THIS SCRIPT DOES:
======================

When gorc.sh is run with an incorrect version (e.g., 9.14.x instead of 9.14.52), it creates:
  1. A "Split off X.Y.Z" commit on the parent branch (candidate-X.Y.x)
  2. A new candidate branch (candidate-X.Y.Z) from that commit
  3. An RC commit on the candidate branch (e.g., "Community Edition X.Y.Z-rc1 Release Candidate 1")
  4. RC tags (e.g., community_X.Y.Z-rc1, eclide_X.Y.Z-rc1, etc.)
For the helm-chart repository, gorc.sh creates an RC commit directly on master (no tags or
candidate branch). When 'hpcc' is specified in the repo list, 'helm-chart' is automatically
included since they are always processed together during release candidate creation.

IMPORTANT: This script ONLY supports rolling back RC1 (first release candidate).
Rolling back RC2+ would require different logic to find and preserve RC1 artifacts

If you need to rollback RC2 or higher, you must do it manually or extend this script.
This script reverses ALL of these changes in three phases:

PHASE 1: VERIFICATION
---------------------
For each repository:
  - Fetches latest from origin to ensure up-to-date refs
  - Checks out candidate-X.Y.Z branch (if not already on it)
  - Verifies HEAD commit is the RC commit (matches expected pattern)
  - Verifies no unexpected commits exist after the RC commit
  - Verifies expected RC tags exist
  - Verifies parent branch (candidate-X.Y.x) exists and has the "Split off X.Y.Z" commit
  - Checks which tags/branches exist on remote for proper cleanup

PHASE 2: LOCAL ROLLBACK
------------------------
For each repository (with confirmation):
  - Deletes RC tags locally (e.g., community_X.Y.Z-rc1)
  - Switches to parent branch (candidate-X.Y.x)
  - Deletes the candidate-X.Y.Z branch
  - Resets parent branch to HEAD~1 (removes "Split off X.Y.Z" commit)

Final state: You're on candidate-X.Y.x at the commit BEFORE the "Split off" was added.

PHASE 3: PUSH TO REMOTE
-----------------------
For each repository (with confirmation):
  - Deletes remote RC tags
  - Force pushes parent branch (uses --force-with-lease for safety)
  - Deletes remote candidate branch

SAFETY FEATURES:
================
  - Validates that version is RC1 (rejects RC2, RC3, etc.)
  - --dry-run mode to preview all operations without making changes
  - Comprehensive verification before any modifications
  - User confirmation required at each phase
  - Detailed logging of all operations
  - Checks for unexpected state (extra commits, wrong branch, etc.)

USAGE:
======
  ./rollback_rc.py [--dry-run] <version> [repo1 repo2 ...]

Examples:
  ./rollback_rc.py --dry-run 9.14.54-rc1
  ./rollback_rc.py 9.14.54-rc1 eclide HPCC-JAPIs ln hpcc
  export all='eclide HPCC-JAPIs ln hpcc' && ./rollback_rc.py 9.14.54-rc1

REQUIREMENTS:
=============
  - Must be run from within the HPCC-Platform repository tools/tagging directory
  - Environment variable 'gitroot' should point to parent directory containing all repos
    (or script will attempt to auto-detect)
  - Repositories should have the candidate-X.Y.Z branch available (locally or on origin)
    The script will automatically fetch from origin and checkout the branch if needed
"""

import os
import sys
import subprocess
import re
from datetime import datetime
from pathlib import Path


class Color:
    """ANSI color codes for terminal output"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'


class RollbackManager:
    """Manages the rollback of RC tags and commits across multiple repositories"""

    def __init__(self, version, git_root, repos, dry_run=False):
        self.version = version
        self.git_root = Path(git_root)
        self.log_file = self.git_root / f"rollback_{version}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.repos = repos
        self.rollback_actions = {}
        self.dry_run = dry_run

        # Parse version to extract components
        match = re.match(r'(\d+\.\d+\.\d+)-rc(\d+)', version)
        if not match:
            self.error(f"Invalid version format: {version}. Expected format: X.Y.Z-rcN")
            sys.exit(1)

        self.base_version = match.group(1)
        self.rc_number = match.group(2)

        # Validate that this is RC1
        if self.rc_number != '1':
            self.error(
                f"This script only supports rolling back RC1 (first release candidate).\n"
                f"You specified: {version} (RC{self.rc_number})\n\n"
                f"Why RC1 only?\n"
                f"  - RC1 has a predictable state: RC commit at HEAD, 'Split off' at HEAD~1\n"
                f"  - RC2+ would have additional commits (RC1, possibly fixes) before RC2\n"
                f"  - This script's verification expects RC commit to be the ONLY commit after split\n"
                f"  - Rolling back RC2+ requires different logic to preserve RC1 artifacts\n\n"
                f"To rollback RC{self.rc_number}, you must manually revert or extend this script."
            )
            sys.exit(1)

        self.branch_name = f"candidate-{self.base_version}"

        # Initialize log file
        mode_str = "DRY-RUN " if self.dry_run else ""
        self.log(f"{mode_str}Rollback initiated for version {version}")
        self.log(f"Base version: {self.base_version}, RC: {self.rc_number}, Branch: {self.branch_name}")
        self.log(f"Repositories: {', '.join(self.repos)}")

    def log(self, message):
        """Write message to log file and print to console"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        with open(self.log_file, 'a') as f:
            f.write(log_entry + '\n')
        print(log_entry)

    def info(self, message):
        """Print info message in blue"""
        print(f"{Color.BLUE}{message}{Color.END}")

    def success(self, message):
        """Print success message in green"""
        print(f"{Color.GREEN}✓ {message}{Color.END}")

    def warning(self, message):
        """Print warning message in yellow"""
        print(f"{Color.YELLOW}⚠ {message}{Color.END}")

    def error(self, message):
        """Print error message in red"""
        print(f"{Color.RED}✗ {message}{Color.END}")

    def run_git_command(self, repo_path, command, capture_output=True, allow_in_dry_run=False):
        """Run a git command in the specified repository

        Args:
            repo_path: Path to repository
            command: Git command to run
            capture_output: Whether to capture and return output
            allow_in_dry_run: If True, run command even in dry-run mode (for read-only operations)
        """
        if self.dry_run and not allow_in_dry_run:
            # In dry-run mode, just log what would be done for write operations
            self.log(f"[DRY-RUN] Would run: {command}")
            if capture_output:
                self.info(f"[DRY-RUN] Would run: {command}")
                return ""  # Return empty string for dry-run
            else:
                self.info(f"[DRY-RUN] Would run: {command}")
                return None

        # Log the command being executed
        self.log(f"Executing in {str(repo_path)}: {command}")

        try:
            result = subprocess.run(
                command,
                cwd=str(repo_path),  # Convert Path to string for subprocess
                shell=True,
                capture_output=capture_output,
                text=True,
                check=True
            )
            if capture_output and result.stdout:
                self.log(f"Command output: {result.stdout.strip()}")
            if result.stderr:
                self.log(f"Command stderr: {result.stderr.strip()}")
            return result.stdout.strip() if capture_output else None
        except subprocess.CalledProcessError as e:
            raise Exception(f"Git command failed: {command}\nError: {e.stderr}")

    def confirm(self, message):
        """Ask user for confirmation"""
        while True:
            response = input(f"{Color.CYAN}{message} (y/n): {Color.END}").lower().strip()
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please answer 'y' or 'n'")

    def is_helm_chart(self, repo_name):
        """Check if this is the helm-chart repository"""
        return repo_name == 'helm-chart'

    def get_tag_patterns(self, repo_name):
        """Determine tag patterns for a repository"""
        # Known patterns for specific repos
        if repo_name == 'eclide':
            return [f"eclide_{self.version}"]
        elif repo_name == 'HPCC-JAPIs':
            return [f"hpcc4j_{self.base_version}-{self.rc_number}-SNAPSHOT"]
        elif repo_name == 'ln':
            return [f"enterprise_{self.version}", f"internal_{self.version}"]
        elif repo_name == 'hpcc':
            return [f"community_{self.version}"]
        elif repo_name == 'helm-chart':
            # helm-chart doesn't use tags, just commits
            return []
        else:
            # Default to community pattern for unknown repos
            # This matches the default commit pattern behavior
            return [f"community_{self.version}"]

    def get_commit_pattern(self, repo_name):
        """Determine commit message pattern for a repository"""
        # Known patterns for specific repos
        patterns = {
            'eclide': r'ECL IDE .* Release Candidate',
            'HPCC-JAPIs': r'.*-SNAPSHOT Release Candidate',
            'ln': r'Enterprise Edition .* Release Candidate',
            'hpcc': r'Community Edition .* Release Candidate',
            'helm-chart': r'HPCC Helm Charts .* Release Candidate'
        }

        # Return known pattern or default to hpcc/community pattern
        return patterns.get(repo_name, r'Community Edition .* Release Candidate')

    def verify_repo(self, repo_name):
        """Verify the repository state before rollback"""
        repo_path = self.git_root / repo_name

        if not repo_path.exists():
            raise Exception(f"Repository not found: {repo_path}")

        self.info(f"\n{'='*60}")
        self.info(f"Verifying repository: {repo_name}")
        self.info(f"{'='*60}")

        # Fetch latest from origin to ensure we have up-to-date refs
        self.info(f"Fetching latest from origin...")
        self.run_git_command(repo_path, "git fetch origin", allow_in_dry_run=True)
        self.success(f"Fetched from origin")

        # helm-chart uses master branch, others use candidate-X.Y.Z
        target_branch = 'master' if self.is_helm_chart(repo_name) else self.branch_name

        # Get current branch (allow in dry-run since it's read-only)
        current_branch = self.run_git_command(repo_path, "git rev-parse --abbrev-ref HEAD", allow_in_dry_run=True)
        self.log(f"{repo_name}: Current branch: {current_branch}")

        # If not on the target branch, try to check it out
        if current_branch != target_branch:
            # Check if the branch exists locally or on remote
            local_branch_exists = self.run_git_command(repo_path, f"git rev-parse --verify {target_branch} 2>/dev/null || echo ''", allow_in_dry_run=True)
            remote_branch_exists = self.run_git_command(repo_path, f"git rev-parse --verify origin/{target_branch} 2>/dev/null || echo ''", allow_in_dry_run=True)

            if local_branch_exists or remote_branch_exists:
                self.info(f"Not on {target_branch}, attempting to check it out...")
                self.run_git_command(repo_path, f"git checkout {target_branch}", allow_in_dry_run=True)
                current_branch = target_branch
                self.success(f"Checked out {target_branch}")
            else:
                raise Exception(f"Repository {repo_name} is not on branch {target_branch} (current: {current_branch}) and branch does not exist")

        # Get the latest commit message (allow in dry-run since it's read-only)
        commit_msg = self.run_git_command(repo_path, "git log -1 --pretty=%B", allow_in_dry_run=True)
        self.log(f"{repo_name}: Latest commit message: {commit_msg}")

        # Verify the commit message matches the RC pattern
        pattern = self.get_commit_pattern(repo_name)
        if not re.search(pattern, commit_msg):
            raise Exception(
                f"Latest commit in {repo_name} does not match expected RC pattern.\n"
                f"Expected pattern: {pattern}\n"
                f"Actual message: {commit_msg}"
            )
        self.success(f"Commit message verified: matches RC pattern")

        # Also verify the version matches (especially important for helm-chart)
        if self.version not in commit_msg:
            raise Exception(
                f"Latest commit in {repo_name} does not contain version {self.version}.\n"
                f"Actual message: {commit_msg}"
            )

        # Check if there are any local commits after the RC commit
        # This would happen if someone made commits after running gorc.sh
        try:
            # Check if there are unpushed commits ahead of the remote branch (allow in dry-run)
            commits_ahead = self.run_git_command(repo_path, f"git rev-list origin/{target_branch}..HEAD --count", allow_in_dry_run=True)
            if int(commits_ahead) > 0:
                raise Exception(
                    f"There are {commits_ahead} unpushed commit(s) after the RC commit in {repo_name}. "
                    f"This is unexpected and rollback is aborted for safety."
                )
        except subprocess.CalledProcessError:
            # If this fails, the branch might not exist on remote yet, which is OK
            self.warning(f"Could not check remote branch - may not exist on origin yet")

        # Verify tags exist (helm-chart has no tags)
        tags_to_delete = self.get_tag_patterns(repo_name)
        existing_tags = []
        missing_tags = []

        if tags_to_delete:  # Only check tags if repo uses them
            for tag in tags_to_delete:
                tag_exists = self.run_git_command(repo_path, f"git tag -l {tag}", allow_in_dry_run=True)
                if tag_exists:
                    existing_tags.append(tag)
                    self.success(f"Tag found: {tag}")
                else:
                    missing_tags.append(tag)
                    self.warning(f"Tag not found: {tag}")

            if not existing_tags:
                raise Exception(f"No expected tags found in {repo_name}. Expected: {tags_to_delete}")

            if missing_tags:
                self.warning(f"Some tags are missing: {missing_tags}")

        # Get commit hash for logging (allow in dry-run)
        commit_hash = self.run_git_command(repo_path, "git rev-parse HEAD", allow_in_dry_run=True)

        # Check if tags exist on remote (allow in dry-run)
        remote_tags = {}
        for tag in existing_tags:
            try:
                remote_ref = self.run_git_command(repo_path, f"git ls-remote origin refs/tags/{tag}", allow_in_dry_run=True)
                remote_tags[tag] = bool(remote_ref)
            except (subprocess.CalledProcessError, Exception):
                remote_tags[tag] = False

        # Check if branch exists on remote (allow in dry-run)
        try:
            remote_branch = self.run_git_command(repo_path, f"git ls-remote origin {target_branch}", allow_in_dry_run=True)
            branch_on_remote = bool(remote_branch)
        except (subprocess.CalledProcessError, Exception):
            branch_on_remote = False

        # Verify parent branch has the "Split off" commit (not needed for helm-chart)
        parent_branch = None
        parent_commit_hash = None
        parent_on_remote = False

        if not self.is_helm_chart(repo_name):
            parent_branch = f"candidate-{self.base_version.rsplit('.', 1)[0]}.x"
            try:
                # Check if parent branch exists locally
                local_parent_exists = self.run_git_command(repo_path, f"git rev-parse --verify {parent_branch} 2>/dev/null || echo ''", allow_in_dry_run=True)

                # If it doesn't exist locally but exists on remote, track it
                if not local_parent_exists:
                    remote_parent_exists = self.run_git_command(repo_path, f"git rev-parse --verify origin/{parent_branch} 2>/dev/null || echo ''", allow_in_dry_run=True)
                    if remote_parent_exists:
                        self.info(f"Parent branch {parent_branch} not found locally, creating from origin/{parent_branch}")
                        self.run_git_command(repo_path, f"git branch {parent_branch} origin/{parent_branch}", allow_in_dry_run=True)
                    else:
                        raise Exception(f"Parent branch {parent_branch} does not exist locally or on origin")

                # Get parent branch's latest commit message (now guaranteed to exist locally and be up-to-date from fetch)
                parent_commit_msg = self.run_git_command(repo_path, f"git log {parent_branch} -1 --pretty=%B", allow_in_dry_run=True)
                parent_commit_hash = self.run_git_command(repo_path, f"git rev-parse {parent_branch}", allow_in_dry_run=True)

                # Verify it's a "Split off" commit for this version
                expected_split_msg = f"Split off {self.base_version}"
                if expected_split_msg not in parent_commit_msg:
                    raise Exception(
                        f"Parent branch {parent_branch} does not have expected 'Split off {self.base_version}' commit.\n"
                        f"Latest commit on {parent_branch}: {parent_commit_msg}"
                    )
                self.success(f"Parent branch {parent_branch} verified: has 'Split off {self.base_version}' commit")

                # Check if parent branch exists on remote
                try:
                    remote_parent = self.run_git_command(repo_path, f"git ls-remote origin {parent_branch}", allow_in_dry_run=True)
                    parent_on_remote = bool(remote_parent)
                except (subprocess.CalledProcessError, Exception):
                    parent_on_remote = False

            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to verify parent branch {parent_branch} in {repo_name}: {str(e)}")

        # Store rollback actions for this repo
        self.rollback_actions[repo_name] = {
            'repo_path': repo_path,
            'current_branch': current_branch,
            'target_branch': target_branch,
            'commit_hash': commit_hash,
            'commit_msg': commit_msg,
            'tags': existing_tags,
            'remote_tags': remote_tags,
            'branch_on_remote': branch_on_remote,
            'parent_branch': parent_branch,
            'parent_commit_hash': parent_commit_hash,
            'parent_on_remote': parent_on_remote
        }

        self.success(f"Repository {repo_name} verified successfully")
        return True

    def perform_local_rollback(self, repo_name):
        """Perform local rollback operations for a repository"""
        actions = self.rollback_actions[repo_name]
        repo_path = actions['repo_path']
        target_branch = actions['target_branch']

        self.info(f"\n{'='*60}")
        self.info(f"Rolling back repository: {repo_name}")
        self.info(f"{'='*60}")

        # Display what will be done
        print(f"\n{Color.BOLD}Actions to perform:{Color.END}")

        if self.is_helm_chart(repo_name):
            # helm-chart: just reset master
            print(f"  • Reset {target_branch} to HEAD~1 (remove RC commit)")
        else:
            # Standard repos: delete tags, delete candidate branch, reset parent
            parent_branch = actions['parent_branch']
            print(f"  • Delete local tags: {', '.join(actions['tags'])}")
            print(f"  • Delete local branch: {self.branch_name}")
            print(f"  • Reset {parent_branch} to remove 'Split off {self.base_version}' commit")

        if not self.confirm(f"\nProceed with local rollback for {repo_name}?"):
            self.warning(f"Skipping {repo_name}")
            return False

        try:
            if self.is_helm_chart(repo_name):
                # helm-chart: simple reset of master
                self.run_git_command(repo_path, "git reset --hard HEAD~1")
                self.log(f"{repo_name}: Reset {target_branch} to HEAD~1")
                self.success(f"Reset {target_branch} to remove RC commit")
            else:
                # Standard repos: delete tags, switch to parent, delete candidate, reset parent
                # Delete local tags
                for tag in actions['tags']:
                    self.run_git_command(repo_path, f"git tag -d {tag}")
                    self.log(f"{repo_name}: Deleted local tag: {tag}")
                    self.success(f"Deleted local tag: {tag}")

                parent_branch = actions['parent_branch']

                # Switch to parent branch FIRST (before deleting the candidate branch)
                self.run_git_command(repo_path, f"git checkout {parent_branch}")
                self.log(f"{repo_name}: Switched to branch: {parent_branch}")
                self.success(f"Switched to branch: {parent_branch}")

                # Delete the candidate branch locally (no need to reset it first since we're deleting it)
                self.run_git_command(repo_path, f"git branch -D {self.branch_name}")
                self.log(f"{repo_name}: Deleted local branch: {self.branch_name}")
                self.success(f"Deleted local branch: {self.branch_name}")

                # Reset parent branch to remove the "Split off" commit
                self.run_git_command(repo_path, f"git reset --hard HEAD~1")
                self.log(f"{repo_name}: Reset {parent_branch} to remove 'Split off {self.base_version}' commit")
                self.success(f"Reset {parent_branch} to remove 'Split off {self.base_version}' commit")

            # Verify we're on the right branch and show current commit
            new_commit = self.run_git_command(repo_path, "git log -1 --oneline")
            current_display_branch = target_branch if self.is_helm_chart(repo_name) else actions['parent_branch']
            self.log(f"{repo_name}: Now on {current_display_branch}: {new_commit}")
            self.success(f"Now on {current_display_branch}: {new_commit}")

            actions['rollback_completed'] = True
            return True

        except Exception as e:
            self.error(f"Failed to rollback {repo_name}: {str(e)}")
            self.log(f"{repo_name}: ROLLBACK FAILED: {str(e)}")
            actions['rollback_completed'] = False
            return False

    def push_to_remote(self, repo_name):
        """Push rollback changes to remote"""
        actions = self.rollback_actions[repo_name]
        repo_path = actions['repo_path']
        target_branch = actions['target_branch']

        if not actions.get('rollback_completed'):
            self.warning(f"Skipping remote push for {repo_name} (local rollback not completed)")
            return False

        try:
            if self.is_helm_chart(repo_name):
                # helm-chart: just force push master
                self.run_git_command(repo_path, f"git push --force-with-lease origin {target_branch}")
                self.log(f"{repo_name}: Force pushed {target_branch} to origin")
                self.success(f"Force pushed {target_branch} to origin")
            else:
                # Standard repos: delete tags, force push parent, delete candidate branch
                # Delete remote tags
                for tag in actions['tags']:
                    if actions['remote_tags'].get(tag):
                        self.run_git_command(repo_path, f"git push origin :refs/tags/{tag}")
                        self.log(f"{repo_name}: Deleted remote tag: {tag}")
                        self.success(f"Deleted remote tag: {tag}")
                    else:
                        self.info(f"Tag {tag} not on remote, skipping")

                # Force push parent branch to update remote
                parent_branch = actions['parent_branch']
                if actions.get('parent_on_remote'):
                    self.run_git_command(repo_path, f"git push --force-with-lease origin {parent_branch}")
                    self.log(f"{repo_name}: Force pushed {parent_branch} to origin")
                    self.success(f"Force pushed {parent_branch} to origin")
                else:
                    self.warning(f"Parent branch {parent_branch} not on remote, skipping force push")

                # Delete remote branch
                if actions['branch_on_remote']:
                    self.run_git_command(repo_path, f"git push origin --delete {self.branch_name}")
                    self.log(f"{repo_name}: Deleted remote branch: {self.branch_name}")
                    self.success(f"Deleted remote branch: {self.branch_name}")
                else:
                    self.info(f"Branch {self.branch_name} not on remote, skipping")

            return True

        except Exception as e:
            self.error(f"Failed to push changes for {repo_name}: {str(e)}")
            self.log(f"{repo_name}: PUSH FAILED: {str(e)}")
            return False

    def run(self):
        """Execute the complete rollback process"""
        mode_str = " [DRY-RUN MODE]" if self.dry_run else ""
        print(f"\n{Color.BOLD}{Color.MAGENTA}{'='*60}")
        print(f"RC Rollback Tool - Version {self.version}{mode_str}")
        print(f"{'='*60}{Color.END}\n")

        if self.dry_run:
            print(f"{Color.YELLOW}Running in DRY-RUN mode - no changes will be made{Color.END}\n")

        # Phase 1: Verification
        self.log("\n" + "="*60)
        self.log("PHASE 1: VERIFICATION")
        self.log("="*60)
        print(f"\n{Color.BOLD}PHASE 1: VERIFICATION{Color.END}")
        print(f"{'='*60}\n")

        for repo in self.repos:
            try:
                self.verify_repo(repo)
            except Exception as e:
                self.error(f"Verification failed for {repo}: {str(e)}")
                self.log(f"VERIFICATION FAILED for {repo}: {str(e)}")
                return False

        self.success("\nAll repositories verified successfully!")

        # Phase 2: Local Rollback
        self.log("\n" + "="*60)
        self.log("PHASE 2: LOCAL ROLLBACK")
        self.log("="*60)
        print(f"\n{Color.BOLD}PHASE 2: LOCAL ROLLBACK{Color.END}")
        print(f"{'='*60}\n")

        completed_repos = []
        for repo in self.repos:
            if self.perform_local_rollback(repo):
                completed_repos.append(repo)

        if not completed_repos:
            self.error("No repositories were rolled back")
            return False

        # Summary of local changes
        print(f"\n{Color.BOLD}LOCAL ROLLBACK SUMMARY{Color.END}")
        print(f"{'='*60}")
        for repo in completed_repos:
            actions = self.rollback_actions[repo]
            print(f"\n{Color.GREEN}{repo}:{Color.END}")
            if self.is_helm_chart(repo):
                # helm-chart: just reset master
                print(f"  • Reset master (removed RC commit)")
            else:
                # Standard repos: tags, branch, parent branch
                print(f"  • Deleted tags: {', '.join(actions['tags'])}")
                print(f"  • Deleted branch: {self.branch_name}")
                print(f"  • Current branch: candidate-{self.base_version.rsplit('.', 1)[0]}.x")

        # Phase 3: Remote Push
        self.log("\n" + "="*60)
        self.log("PHASE 3: PUSH TO REMOTE")
        self.log("="*60)
        print(f"\n{Color.BOLD}PHASE 3: PUSH TO REMOTE{Color.END}")
        print(f"{'='*60}\n")

        print(f"{Color.YELLOW}This will push the following changes to origin:{Color.END}")
        for repo in completed_repos:
            actions = self.rollback_actions[repo]
            print(f"\n{repo}:")
            if self.is_helm_chart(repo):
                # helm-chart: just force push master
                print(f"  • Force push master (remove RC commit)")
            else:
                # Standard repos: tags, parent branch, candidate branch
                for tag in actions['tags']:
                    if actions['remote_tags'].get(tag):
                        print(f"  • Delete remote tag: {tag}")
                # Parent branch will always be force-pushed
                parent_branch = f"candidate-{self.base_version.rsplit('.', 1)[0]}.x"
                print(f"  • Force push {parent_branch} (remove 'Split off' commit)")
                if actions['branch_on_remote']:
                    print(f"  • Delete remote branch: {self.branch_name}")

        if not self.confirm(f"\n{Color.BOLD}Push all changes to origin?{Color.END}"):
            self.warning("Remote push cancelled by user")
            self.log("Remote push cancelled by user")
            return True  # Local rollback was successful

        push_success = []
        for repo in completed_repos:
            if self.push_to_remote(repo):
                push_success.append(repo)

        # Final Summary
        print(f"\n{Color.BOLD}{Color.GREEN}{'='*60}")
        print(f"ROLLBACK COMPLETE")
        print(f"{'='*60}{Color.END}\n")

        print(f"Successfully rolled back {len(push_success)}/{len(completed_repos)} repositories")
        print(f"\nLog file: {self.log_file}")

        self.log(f"Rollback completed successfully for {len(push_success)} repositories")
        return True


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} [--dry-run] <version> [repo1 repo2 ...]")
        print(f"Example: {sys.argv[0]} 9.14.54-rc1")
        print(f"Example: {sys.argv[0]} --dry-run 9.14.54-rc1")
        print(f"Example: {sys.argv[0]} 9.14.54-rc1 eclide HPCC-JAPIs ln hpcc")
        print(f"\nOptions:")
        print(f"  --dry-run    Show what would be done without making any changes")
        print(f"\nIf repos are not specified, they will be read from the 'all' environment variable")
        sys.exit(1)

    # Parse arguments
    args = sys.argv[1:]
    dry_run = False

    # Check for --dry-run flag
    if '--dry-run' in args:
        dry_run = True
        args.remove('--dry-run')

    if len(args) < 1:
        print(f"{Color.RED}Error: Version argument required{Color.END}")
        print(f"Usage: {sys.argv[0]} [--dry-run] <version> [repo1 repo2 ...]")
        sys.exit(1)

    version = args[0]

    # Get repository list from command line or environment variable
    if len(args) > 1:
        repos = args[1:]
        print(f"Using repositories from command line: {' '.join(repos)}")
    else:
        all_repos = os.environ.get('all')
        if not all_repos:
            print(f"{Color.RED}Error: No repositories specified and 'all' environment variable is not set{Color.END}")
            print(f"\nUsage: {sys.argv[0]} [--dry-run] <version> [repo1 repo2 ...]")
            print(f"   or: export all='eclide HPCC-JAPIs ln hpcc' && {sys.argv[0]} <version>")
            sys.exit(1)
        repos = all_repos.split()
        print(f"Using repositories from 'all' environment variable: {' '.join(repos)}")

    # Auto-add helm-chart when hpcc is being processed
    if 'hpcc' in repos and 'helm-chart' not in repos:
        repos.append('helm-chart')
        print(f"{Color.CYAN}Auto-added helm-chart (processed with hpcc){Color.END}")

    # Determine git root
    git_root = os.environ.get('gitroot')
    if not git_root:
        # Try to find it relative to the script
        script_dir = Path(__file__).parent.parent.parent.parent
        git_root = script_dir.parent

    git_root = os.path.expanduser(git_root)

    print(f"Git root: {git_root}")
    print(f"Version to rollback: {version}")
    if dry_run:
        print(f"{Color.CYAN}Mode: DRY-RUN (no changes will be made){Color.END}")

    manager = RollbackManager(version, git_root, repos, dry_run=dry_run)

    try:
        success = manager.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n{Color.YELLOW}Rollback cancelled by user{Color.END}")
        manager.log("Rollback cancelled by user (KeyboardInterrupt)")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Color.RED}Fatal error: {str(e)}{Color.END}")
        manager.log(f"FATAL ERROR: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
