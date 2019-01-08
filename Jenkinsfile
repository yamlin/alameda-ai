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
      pip install filelock
      pip install influxdb
      export PYTHONPATH=${env.WORKSPACE}
      export
      pytest --junit-xml=test_results.xml tests
      #python3.6 -B -m unittest discover -v
    """
    junit keepLongStdio: true, allowEmptyResults: true, testResults: 'test_results.xml'
  }
}
