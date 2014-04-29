import time
import boto
import os
import subprocess
import tempfile
import shutil
import datetime

def make_backup():
    repo_names = os.listdir("/home/git/repositories")
    tmpdir = tempfile.mkdtemp()
    conn = boto.connect_s3()
    
    hour = datetime.datetime.now().hour
    
    try:
        for repo_name in repo_names:
            if not repo_name.endswith(".git"):
                continue
            
            repo = "/home/git/repositories/{}".format(repo_name)
            backup_file = "{}/{}.bundle".format(tmpdir, repo_name)
            
            subprocess.check_call(["/usr/bin/git", "bundle", "create",
                backup_file, "master"],
                env = {"GIT_DIR": repo})
            
            bucket = conn.get_bucket("ml-git-backups")
            key = bucket.new_key("{}/{}.bundle".format(repo_name, hour))
            key.set_contents_from_filename(backup_file)
    
    finally:
        shutil.rmtree(tmpdir)

def main():
    while True:
        make_backup()
        time.sleep(3600)

if __name__ == '__main__':
    main()