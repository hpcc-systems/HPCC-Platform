import paramiko
import os

HOST_NAME = os.getenv('HOSTNAME')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT') 


def sftp_put_nested_dir(local_path, remote_path, hostname=HOST_NAME, port=PORT, username=USERNAME, password=PASSWORD):
    """Uploads a local directory to a remote server via SFTP.

    Args:
    local_path: The path to the local directory to upload.
        remote_path: The path on the remote server where the directory should be created.
        hostname: The hostname or IP address of the remote server.
        port: The port number for the SSH connection
        username: The username for authentication.
        password: The password for authentication.
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh_client.connect(hostname, port, username, password, look_for_keys=False)
        sftp_client = ssh_client.open_sftp()
        
        print("\tcwd: ")
        for item in os.listdir(local_path):
            print("\t"+ item)

        for item in os.listdir(local_path):
            local_item_path = os.path.join(local_path, item).replace("\\", "/")
            remote_item_path = os.path.join(remote_path, item).replace("\\", "/")
            print("local_item_path: " + local_item_path)
            print("remote_item_path: " + remote_item_path)
            if os.path.isfile(local_item_path):
                print("Copying file: {0} to {1}".format(local_item_path, remote_item_path))
                sftp_client.put(local_item_path, remote_item_path)
            elif os.path.isdir(local_item_path):
                try:
                    print("Creating directory: {0}".format(remote_item_path))
                    sftp_client.mkdir(remote_item_path)
                except IOError:
                    # Directory might already exist
                    pass
                sftp_put_nested_dir(local_item_path, remote_item_path, hostname, port, username, password)
        
    except Exception as e:
         print(f"An error occurred: {e}")
    finally:
        sftp_client.close()
        ssh_client.close()
