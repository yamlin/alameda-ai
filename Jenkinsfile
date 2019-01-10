node('python36') {
  stage('checkout') {
        git branch: 'master', url: "https://github.com/containers-ai/alameda-ai.git"
  }
  stage("Test") {
    sh """
      python3.6 --version
      python3.6 -m venv venv
      source venv/bin/activate
      pip install --upgrade pip
      pip install --upgrade setuptools
      pip install -r requirements.txt
      pip install pytest
      pip install pytest-cov
      pip install filelock
      pip install influxdb
      export PYTHONPATH=./
      export
      pytest --cov-config=.coveragerc --cov=./
      curl -s https://codecov.io/bash | bash -s - -t 8de5eed5-52c8-4db8-99cf-04d1fb68f8b3
    """
  }
  stage("Lint") {
    sh """
      source venv/bin/activate
      pip install pylint
      export PYTHONPATH=./
      cd services
      pylint ./
      cd ../framework
      pylint ./
      cd ../tests
      pylint ./
    """
  }
}
