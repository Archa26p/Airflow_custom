import os,time,shutil
path=r"/cloudapps/airflow/airflow_venv/logs"
for f in os.listdir(path):
    if os.stat(os.path.join(path,f)).st_mtime < time.time() - 7 * 86400:
        print(f)
        shutil.rmtree(os.path.join(path,f))
        
path="/cloudapps/airflow/airflow_venv/logs/scheduler"
for f in os.listdir(path):
    if os.stat(os.path.join(path, f)).st_mtime < time.time() - 7 * 86400:
        print(f)
        shutil.rmtree(os.path.join(path, f))
