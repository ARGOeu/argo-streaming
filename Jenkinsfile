
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
            post {
                always {
                    cleanWs()
                }
            }
        }
        stage('Flink Jobs v2 Testing & Packaging') {
            agent {
                docker {
                    image 'argo.registry:5000/epel-7-java18'
                    args '-u jenkins:jenkins'
                }
            }
            steps {
                echo 'Packaging & Testing Flink Jobs'
                sh """
                mvn clean package cobertura:cobertura -Dcobertura.report.format=xml -f ${PROJECT_DIR}/flink_jobs_v2/pom.xml
                """
                junit '**/target/surefire-reports/*.xml'
                cobertura coberturaReportFile: '**/target/site/cobertura/coverage.xml'
                archiveArtifacts artifacts: '**/target/*.jar'
            }
            post {
                always {
                    cleanWs()
                }
            }
        }
        stage('Flink Jobs v3 Testing & Packaging') {
            agent {
                docker {
                    image 'argo.registry:5000/epel-7-java11-mvn384'
                    args '-u jenkins:jenkins -v $HOME/.m2:/root/.m2 -v /var/run/docker.sock:/var/run/docker.sock -u root:root'
                }
            }
            steps {
                 sh """
                cd ${PROJECT_DIR}/flink_jobs_v3/
                mvn clean package
                """
                junit '**/target/surefire-reports/*.xml'
                archiveArtifacts artifacts: '**/target/*.jar'
                step( [ $class: 'JacocoPublisher' ] )
            }
            post {
                always {
                    cleanWs()
                }
            }
        }
    }
    post {
        success {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rocket: New version for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME !")
                }
            }
        }
        failure {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rain_cloud: Build Failed for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME")
                }
            }
        }
    }
}
