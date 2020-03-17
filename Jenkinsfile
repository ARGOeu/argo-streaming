
pipeline {
    agent {
        docker {
            image 'argo.registry:5000/epel-7-py36'
            args '-u jenkins:jenkins'
        }
    }
    options {
        checkoutToSubdirectory('argo-streaming')
        newContainerPerStage()
    }
    environment {
        PROJECT_DIR='argo-streaming'
        REQUIREMENTS="${PROJECT_DIR}/bin/requirements.txt"
    }
    stages {
        stage('Configuration scripts Tests') {
            steps {
                echo 'Testing compute engine auto configuration scripts'
                sh """
                pip3 install -r ${REQUIREMENTS} --user
                pytest --junit-xml=${PROJECT_DIR}/junit.xml --cov=${PROJECT_DIR} --cov-report=xml
                """
                junit '**/junit.xml'
                cobertura coberturaReportFile: '**/coverage.xml'
            }
        }
    }
}
