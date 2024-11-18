################################################################################
#    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

import os
import subprocess
from datetime import datetime, timedelta
import re
import configparser
import requests
from requests.auth import HTTPBasicAuth
from cryptography.fernet import Fernet
import logging
from pwd import getpwnam
from grp import getgrnam
import argparse
import zipfile

script_dir = os.getcwd()

parser = argparse.ArgumentParser(
  description='Fetch HPCCSystems assets from GitHub and JFrog',
  prog='fetch_assets.py'
)

parser.add_argument('-k', '--key_file', help='Path to key file', required=False)
parser.add_argument('-c', '--config_file', help='Path to configuration file', required=False)
parser.add_argument('-t', '--tag', action='append', help='Tag to fetch assets for', required=False)
parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging', required=False)
args = parser.parse_args()


# Read the configuration file
config = configparser.ConfigParser()
config_file = os.path.join('/etc/hpccsystems', 'fetch_assets.ini')
if args.config_file:
  config_file = args.config_file
if not config.read(config_file):
  print(f"Configuration file not found: {config_file}")
  exit(1)

# Define the path to the local GitHub repository
repo_url = config.get('repository', 'url')
repo_path = config.get('repository', 'path')
repository_name = config.get('repository', 'name')
repository_owner = config.get('repository', 'owner')
encryption_key = config.get('security', 'key_path')
if args.key_file:
  encryption_key = args.key_file
jfrog_username = config.get('jfrog', 'username')
encrypted_jfrog_key = config.get('jfrog', 'encrypted_token')
jfrog_base_url = config.get('jfrog', 'base_url')
data_dir_base = config.get('local', 'data_dir_base')
logdir = config.get('local', 'logdir')
local_username = config.get('local', 'username')
local_groupname = config.get('local', 'groupname')

log_level=logging.INFO
if args.verbose:
  log_level=logging.DEBUG

# Configure logging
logging.basicConfig(
  filename=f"{logdir}/fetch_assets.log",
  level=log_level,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

# declare get_asset function
def download_asset(url, username=None, decrypted_password=None):
  filename = os.path.basename(url)
  logging.debug(f"Attempting download of {url}")
  if not username or not decrypted_password:
    response = requests.get(url)
  else:
    response = requests.get(url, auth=HTTPBasicAuth(username, decrypted_password))
  if response.status_code == 200:
    with open(f"{filename}", 'wb') as f:
      f.write(response.content)
    logging.info(f"Downloaded {filename}")
  elif response.status_code == 404:
    logging.debug(f"Asset not found: {url}")
  else:
    logging.error(f"{response.status_code} Failed to fetch {filename}")

# Recursively change ownership of files and directories
def chown_recursive(path, uid, gid):
  for root, dirs, files in os.walk(path):
    for dir in dirs:
      os.chown(os.path.join(root, dir), uid, gid)
    for file in files:
      os.chown(os.path.join(root, file), uid, gid)

logging.debug(f"Generating cipher suite from {encryption_key}")
with open(encryption_key, 'rb') as key_file:
  key = key_file.read()
cipher_suite = Fernet(key)
decrypted_jfrog_password = cipher_suite.decrypt(encrypted_jfrog_key.encode()).decode()

# Get the current date and time
current_time = datetime.now()
logging.debug(f"Current time is {current_time}")

# Get the date and time 24 hours ago
time_24_hours_ago = current_time - timedelta(hours=24)
logging.debug(f"24 hours ago was {time_24_hours_ago}")

local_gid = getgrnam(local_groupname).gr_gid
local_uid = getpwnam(local_username).pw_uid
logging.debug(f"UID: {local_uid}, GID: {local_gid}")

version_list = []
if args.tag:
  for tag in args.tag:
    version_list.append(re.sub(r'^community_', '', tag))
else:
  logging.debug(f"Repo path is {repo_path}")
  # Navigate to the local GitHub repository
  if not os.path.isdir(repo_path):
    logging.info("Repository not found, cloning")
    repo_head = os.path.split(repo_path)[0]
    result = subprocess.run(
      ['git', 'clone', 'https://github.com/hpcc-systems/hpcc-platform.git'],
      capture_output=True, text=True, cwd=repo_head
    )
    for line in result.stderr.splitlines():
      logging.debug(line)

  # Refresh the local repository
  result = subprocess.run(
    ['git', 'fetch', '--all', '--tags'],
    capture_output=True, text=True, cwd=repo_path
  )
  for line in result.stderr.splitlines():
    logging.debug(line)

  # List all tags and their creation dates, filter tags created in the last 24 hours
  result = subprocess.run(
    ['git', 'for-each-ref', '--sort=creatordate', '--format=%(refname:short) %(creatordate:unix)', 'refs/tags'],
    capture_output=True, text=True, cwd=repo_path
  )
  for line in result.stderr.splitlines():
    logging.debug(line)

  for line in result.stdout.splitlines():
    tag, timestamp = line.split()
    timestamp = int(timestamp)
    tag_time = datetime.fromtimestamp(timestamp)
    if time_24_hours_ago <= tag_time <= current_time:
      logging.info(f"Tag: {tag}, Created: {tag_time}")
      version_list.append(re.sub(r'^community_', '', tag))

package_list = ['platform', 'clienttools', 'eclide']
redhat_distributions = ['el7', 'el8', 'rocky8', 'amzn2']
debian_distributions = ['noble', 'focal', 'bionic', 'jammy']
plugin_list = [
  'cassandraembed',
  'couchbaseembed',
  'eclblas',
  'h3',
  'javaembed',
  'kafka',
  'memcached',
  'mongodbembed',
  'mysqlembed',
  'nlp',
  'parquetembed',
  'redis',
  'sqlite3embed',
  'sqs',
  'wasmembed'
]
documentation_languages = ['EN_US', 'PT_BR']

if not version_list:
  logging.info("No new tags found")
  exit(0)

for version in version_list:
  logging.info(f"Version: {version}")
  base_version = version.split('-')[0]

  # Fetch assets from GitHub
  logging.info(f"Creating {data_dir_base}/CE-Candidate-{base_version} bin/platform, bin/clienttools, bin/ide, bin/plugins and docs directories")
  os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/bin/platform", exist_ok=True)
  os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/bin/clienttools", exist_ok=True)
  os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/bin/ide", exist_ok=True)
  os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/bin/plugins", exist_ok=True) 
  os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/docs", exist_ok=True)
  # do CE platform
  os.chdir(f"{data_dir_base}/CE-Candidate-{base_version}/bin/platform")
  for distribution in redhat_distributions:
    download_asset(f"{repo_url}/download/community_{version}/hpccsystems-platform-community_{version}.{distribution}.x86_64_withsymbols.rpm")
  for distribution in debian_distributions:
    download_asset(f"{repo_url}/download/community_{version}/hpccsystems-platform-community_{version}{distribution}_amd64_withsymbols.deb")
    download_asset(f"{repo_url}/download/community_{version}/hpccsystems-platform-community_{version}{distribution}_amd64_k8s.deb")
  # do CE clienttools
  os.chdir(f"{data_dir_base}/CE-Candidate-{base_version}/bin/clienttools")
  for distribution in redhat_distributions:
    download_asset(f"{repo_url}/download/community_{version}/hpccsystems-clienttools-community_{version}.{distribution}.x86_64_withsymbols.rpm")
  for distribution in debian_distributions:
    download_asset(f"{repo_url}/download/community_{version}/hpccsystems-clienttools-community_{version}{distribution}_amd64_withsymbols.deb")
  download_asset(f"{repo_url}/download/community_{version}/hpccsystems-clienttools-community_{version}Darwin-x86_64.pkg")
  download_asset(f"{repo_url}/download/community_{version}/hpccsystems-clienttools-community_{version}Windows-x86_64.exe")
  # do CE plugins
  os.chdir(f"{data_dir_base}/CE-Candidate-{base_version}/bin/plugins")
  for plugin in plugin_list:
    for distribution in redhat_distributions:
      download_asset(f"{repo_url}/download/community_{version}/hpccsystems-plugin-{plugin}_{version}.{distribution}.x86_64_withsymbols.rpm")
    for distribution in debian_distributions:
      download_asset(f"{repo_url}/download/community_{version}/hpccsystems-plugin-{plugin}_{version}{distribution}_amd64_withsymbols.deb")
  # do CE eclide
  os.chdir(f"{data_dir_base}/CE-Candidate-{base_version}/bin/ide")
  download_asset(f"{repo_url}/download/community_{version}/hpccsystems-eclide-community_{version}Windows-i386.exe")
  # do CE docs
  for language in documentation_languages:
    logging.info(f"Creating {data_dir_base}/CE-Candidate-{base_version}/docs/{language} directory")
    os.makedirs(f"{data_dir_base}/CE-Candidate-{base_version}/docs/{language}", exist_ok=True)
    os.chdir(f"{data_dir_base}/CE-Candidate-{base_version}/docs/{language}")
    download_asset(f"{repo_url}/download/community_{version}/ALL_HPCC_DOCS_{language}-{version}.zip")
    download_asset(f"{repo_url}/download/community_{version}/ECL_Code_Files_{language}.zip")
    if os.path.exists(f"ALL_HPCC_DOCS_{language}-{version}.zip"):
      with zipfile.ZipFile(f"ALL_HPCC_DOCS_{language}-{version}.zip", 'r') as zipped_docs:
        zipped_docs.extractall(f"{data_dir_base}/CE-Candidate-{base_version}/docs/{language}")
  chown_recursive(f"{data_dir_base}/CE-Candidate-{base_version}", local_uid, local_gid)


  ## Fetch internal assets from JFrog
  logging.info(f"Creating {data_dir_base}/LN-Candidate-{base_version}/bin/platform-withplugins directory")
  os.makedirs(f"{data_dir_base}/LN-Candidate-{base_version}/bin/platform-withplugins", exist_ok=True)
  os.chdir(f"{data_dir_base}/LN-Candidate-{base_version}/bin/platform-withplugins")
  for distribution in redhat_distributions:
    download_asset(f"{jfrog_base_url}/hpccpl-rpm-virtual/LN/{distribution}/x86_64/hpccsystems-platform-internal_{version}.{distribution}.x86_64_withsymbols.rpm", jfrog_username, decrypted_jfrog_password)
  for distribution in debian_distributions:
    download_asset(f"{jfrog_base_url}/hpccpl-debian-virtual/pool/LN/hpccsystems-platform-internal_{version}{distribution}_amd64_withsymbols.deb", jfrog_username, decrypted_jfrog_password)
  ## internal clienttools
  logging.info(f"Creating {data_dir_base}/LN-Candidate-{base_version}/bin/clienttools directory")
  os.makedirs(f"{data_dir_base}/LN-Candidate-{base_version}/bin/clienttools", exist_ok=True)
  os.chdir(f"{data_dir_base}/LN-Candidate-{base_version}/bin/clienttools")
  for distribution in redhat_distributions:
    download_asset(f"{jfrog_base_url}/hpccpl-rpm-virtual/LN/{distribution}/x86_64/hpccsystems-clienttools-internal_{version}.{distribution}.x86_64_withsymbols.rpm", jfrog_username, decrypted_jfrog_password)
  for distribution in debian_distributions:
    download_asset(f"{jfrog_base_url}/hpccpl-debian-virtual/pool/LN/hpccsystems-clienttools-internal_{version}{distribution}_amd64_withsymbols.deb", jfrog_username, decrypted_jfrog_password)
  download_asset(f"{jfrog_base_url}/hpccpl-macos-virtual/LN/macos/x86_64/hpccsystems-clienttools-internal_{version}Darwin-x86_64.pkg", jfrog_username, decrypted_jfrog_password)
  download_asset(f"{jfrog_base_url}/hpccpl-windows-virtual/LN/windows/x86_64/hpccsystems-clienttools-internal_{version}Windows-x86_64.exe", jfrog_username, decrypted_jfrog_password)
  chown_recursive(f"{data_dir_base}/LN-Candidate-{base_version}", local_uid, local_gid)

os.chdir(script_dir)
logging.info("Assets fetched, exiting")
