Documentation for `builds_file_transfer.py` and `ssh_connection.py`
===================================================================

This document provides instructions on how to use the Python scripts `builds_file_transfer.py` and `ssh_connection.py` to download and transfer files from a GitHub repository to a remote server using SFTP.

* * *

**Overview**
------------

*   `builds_file_transfer.py`: This script fetches a specific release of a GitHub repository, downloads a ZIP file from the release, unzips it locally, and then uploads the extracted files to a remote server via SFTP.
*   `ssh_connection.py`: Contains a function to upload files and directories recursively to a remote server via SFTP using the `paramiko` library.

* * *

**Prerequisites**
-----------------

Before using these scripts, ensure that you have the following:

*   **Python 3.x** installed.
*   **paramiko** library for SSH/SFTP functionality. If not installed, you can install it using:
    
    `pip install paramiko`
    
*   Access to a GitHub repository with releases.
*   SFTP credentials (hostname, username, password) for the remote server.

* * *

**Setup**
---------

1.  **Configure `builds_file_transfer.py`**:
    
    *   Set the `REPO_OWNER`, `REPO_NAME`, and `TAG` variables with appropriate values.
        
            REPO_OWNER = 'your-repo-owner'
            REPO_NAME = 'your-repo-name' 
            TAG = 'release-tag'  # Example: community_9.10.4-1

    *   This script fetches the release information from GitHub based on the repository owner, repository name, and the release tag.
2.  **Configure `ssh_connection.py`**:
    
    *   In the `ssh_connection.py` script, configure the following variables:
        
            hostname = "your.remote.server.com"
            username = "your-username"
            password = "your-password"
            port = 22
        
    *   These credentials will be used for the SFTP connection to the remote server.

* * *

**How to Use**
--------------

### **1\. Running `builds_file_transfer.py`**

This script will:

1.  Fetch the release details from GitHub using the provided repository details.
2.  Check if the release ends with `-1` (indicating a gold release) and fetch the assets (files) from it.
3.  Download the ZIP file (`portal_html_EN_US.zip`) from the release.
4.  Unzip the downloaded file locally.
5.  Use the `sftp_put_directory` function from `ssh_connection.py` to recursively upload the unzipped files to the remote server.

To run the script, simply execute it:

`python builds_file_transfer.py`

* * *

### **2\. `ssh_connection.sftp_put_directory` Function**

The `sftp_put_directory` function is responsible for uploading the files to the remote server. The function works as follows:

*   **local\_path**: Path to the local directory that will be uploaded.
*   **remote\_path**: Path to the target directory on the remote server.
*   **hostname**: The server's hostname or IP address.
*   **username**: The username for SSH login.
*   **password**: The password for SSH login.
*   **port**: The SSH port (default is 22).

This function:

*   Connects to the remote server using SSH.
*   Iterates through the files and directories in the local directory (`local_path`).
*   For each file, it uploads it using the `put` method.
*   For each subdirectory, it creates the corresponding directory on the remote server and recursively uploads its contents.

* * *



**Additional Notes**
--------------------

*   The script assumes that it's a gold build, and that the ZIP file contains the directory `hpcc-dev/`, which is being uploaded to the remote server.
*   Modify the `extract_to` parameter in the `download_and_unzip` function if you wish to extract the files to a different directory than the current one (`.`).
*   Modifications can/will be made to be configured and automated using GitHub Actions

* * *

**Conclusion**
--------------

These scripts allow for easy automation of downloading files from GitHub releases and uploading them to a remote server via SFTP. Customize the repository and SFTP details to suit your needs, and the script will handle the rest!