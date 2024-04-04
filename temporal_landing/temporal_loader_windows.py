import os
import subprocess

def upload_avro_to_hdfs(avro_data_path, hdfs_user, hdfs_password, hdfs_ip, hdfs_port, hdfs_avro_path, overwrite=True):
    for file_name in os.listdir(avro_data_path):
        if file_name.endswith('.avro'):
            file_path = os.path.join(avro_data_path, file_name)
            file_path_windows = file_path.replace('\\', '/')
            if os.path.isfile(file_path_windows):
                print(f"Uploading {file_name} to HDFS...")
                overwrite_flag = '-f' if overwrite else ''
                curl_command = [
                    "curl", "-L", "-i", "-X", "PUT",
                    "-T", file_path_windows,
                    f"http://{hdfs_user}:{hdfs_password}@{hdfs_ip}:{hdfs_port}/webhdfs/v1"
                    f"{hdfs_avro_path}/{file_name}?op=CREATE&overwrite={overwrite_flag}"
                ]
                subprocess.run(curl_command, shell=True)
            else:
                print(f"Error: File {file_path} does not exist.")
