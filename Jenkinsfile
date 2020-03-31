
pipeline {
    agent none
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
              agent {
                  docker {
                      image 'argo.registry:5000/epel-7-py36'
                      args '-u jenkins:jenkins'
                  }
              }
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
        stage('Flink Jobs Testing & Packaging') {
            agent {
                docker {
                    image 'argo.registry:5000/epel-7-java18'
                    args '-u jenkins:jenkins'
                }
            }
            steps {
                echo 'Packaging & Testing Flink Jobs'
                sh """
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs/stream_status/pom.xml
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs/batch_ar/pom.xml
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs/batch_status/pom.xml
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs/ams_ingest_metric/pom.xml
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs/ams_ingest_sync/pom.xml
                """
                junit '**/target/surefire-reports/*.xml'
                cobertura coberturaReportFile: '**/target/site/cobertura/coverage.xml'
                archiveArtifacts artifacts: '**/target/*.jar'
            }
        }
    }
}
