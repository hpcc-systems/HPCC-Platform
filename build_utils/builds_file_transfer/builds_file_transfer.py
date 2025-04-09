import requests
import json
import os
import ssh_connection
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile


REPO_OWNER = os.getenv('REPO_OWNER','hpcc-systems')
REPO_NAME = os.getenv('REPO_NAME','HPCC-Platform')
TAG = os.getenv('TAG')

url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/tags/{TAG}'

response = requests.get(url)
response.raise_for_status()
release = response.json()

# Handle multiple releases, assuming the assets are spread across them
assets = []
assets.extend(release['assets'])

def download_and_unzip(url, extract_to='.'):
    zip_url = urlopen(url)
    zip_file = ZipFile(BytesIO(zip_url.read()))
    zip_url.close()
    zip_file.extractall(path=extract_to)

def extract_zip_url(assets):
    for asset in assets:
        if asset['name'] == 'portal_html_EN_US.zip':
            return asset['browser_download_url']


zip_url = extract_zip_url(assets)
print('Downloading and Unzipping: ' + zip_url)
download_and_unzip(zip_url)
ssh_connection.sftp_put_nested_dir(local_path='./hpcc-dev/build/docs/EN_US/PortalHTML/ECLR_EN_US', remote_path='./ECLR_EN_US/')
ssh_connection.sftp_put_nested_dir(local_path='./hpcc-dev/build/docs/EN_US/PortalHTML/ProgrammersGuide_EN_US', remote_path='./ProgrammersGuide_EN_US/')
ssh_connection.sftp_put_nested_dir(local_path='./hpcc-dev/build/docs/EN_US/PortalHTML/SLR_EN_US', remote_path='./SLR_EN_US/')
