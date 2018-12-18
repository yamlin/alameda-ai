'''Main entrypoint for demo server.'''

import os
import time
import subprocess
from framework.log.logger import Logger


def main():
    '''Main entrypoint for demo server.'''

    svc_rootpath = '/alameda-ai/services/orchestra'
    pyscripts = ['grpc_server.py', 'workload_prediction.py']

    log = Logger()
    log.info("Start demo server.")

    for script in pyscripts:
        log.info("Start to run", script)

        script_path = os.path.join(svc_rootpath, script)
        subprocess.Popen(['python3', script_path])

    while True:
        time.sleep(60)

    log.info("Demo server is completed.")


if __name__ == '__main__':
    main()
