// drop-glue PROVISIONING pipeline (infrastructure only).
//
// Creates, updates or destroys everything the Glue batch jobs need in one
// environment (= Terraform workspace): the job bucket, the Glue jobs, IAM roles,
// log groups, tracker table and the optional rules. Optionally copies
// scripts/*.py into the job bucket.
//
// The pipelines in jenkins/ (find, status, forget, stop) RUN the batches; this
// one only provisions. See docs/GUIDE.md section 7.2.1.
//
// Flow: validate → (prod: change request number) → checkout → init → plan
//       → APPROVAL (10 min) → apply/destroy → copy scripts → outputs
//
// Environment variables (set on the Jenkins job, folder or agent):
//   ROLE_NAME           IAM role to assume for every AWS / Terraform operation
//   ROLE_ACCOUNT        AWS account ID (12 digits) of that role
//   EXTERNAL_ID         external ID required by the role's trust policy
//   AWS_REGION          region of the deployment (overrides `region` in tfvars)
//   NOTIFICATION_EMAIL  who is emailed for approvals and results (also passed to the jobs as notify_to)
//   APPROVERS           optional: comma-separated Jenkins users/groups allowed to approve
//
// Agent / plugin requirements: Terraform >= 1.10, AWS CLI v2, Pipeline: AWS Steps
// (withAWS), Pipeline Utility Steps (readJSON), Git, Mailer (mail).

pipeline {
  agent any

  options {
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    buildDiscarder(logRotator(numToKeepStr: '50'))
    timeout(time: 90, unit: 'MINUTES')
  }

  parameters {
    string(name: 'BRANCH', defaultValue: 'main', description: 'Git branch (or tag) of this repository to check out')
    choice(name: 'TERRAFORM_WORKSPACE', choices: ['stage', 'uat', 'prod'], description: 'Terraform workspace = environment. Every resource name ends in -<workspace>; uses backend/<workspace>.s3.tfbackend and tfvars/<workspace>.tfvars. prod asks for a change request number.')
    choice(name: 'ACTION', choices: ['apply', 'destroy'], description: 'apply = create/update the infrastructure; destroy = delete ALL of it in this workspace')
    booleanParam(name: 'DESTROY_ALL', defaultValue: false, description: 'REQUIRED for ACTION=destroy. Confirms deleting every resource of this workspace, INCLUDING the job bucket with all scripts, input files and reports (all versions).')
    booleanParam(name: 'COPY_SCRIPTS', defaultValue: false, description: 'ACTION=apply only: copy scripts/*.py into the job bucket (tick it when the scripts changed). Done automatically on the first apply.')
  }

  environment {
    TF_IN_AUTOMATION = '1'
    TF_INPUT         = '0'
  }

  stages {
    stage('Validate') {
      steps {
        script {
          // ---- environment variables provided by Jenkins
          ['ROLE_NAME', 'ROLE_ACCOUNT', 'EXTERNAL_ID', 'AWS_REGION', 'NOTIFICATION_EMAIL'].each { name ->
            if (!env[name]?.trim()) { error("Environment variable ${name} is not set on this Jenkins job") }
          }
          if (!(env.ROLE_ACCOUNT ==~ /\d{12}/)) { error('ROLE_ACCOUNT must be a 12-digit AWS account ID') }
          if (!(env.ROLE_NAME ==~ /[\w+=,.@\/-]{1,64}/)) { error('ROLE_NAME must be an IAM role name') }
          if (!(env.AWS_REGION ==~ /[a-z]{2}(-[a-z]+)+-\d/)) { error("AWS_REGION '${env.AWS_REGION}' is not a region name") }
          if (!(env.NOTIFICATION_EMAIL ==~ /[^@\s,]+@[^@\s,]+(\s*,\s*[^@\s,]+@[^@\s,]+)*/)) {
            error('NOTIFICATION_EMAIL must be one or more comma-separated email addresses')
          }

          // ---- build parameters
          if (!(params.BRANCH ==~ /[A-Za-z0-9._\/-]{1,200}/)) { error('BRANCH must be a branch or tag name') }
          if (params.ACTION == 'destroy' && !params.DESTROY_ALL) {
            error("ACTION=destroy needs DESTROY_ALL to be ticked. It deletes EVERY resource of workspace '${params.TERRAFORM_WORKSPACE}', including the job bucket and all its reports.")
          }
          if (params.ACTION == 'destroy' && params.COPY_SCRIPTS) { error('COPY_SCRIPTS can only be used with ACTION=apply') }
          if (params.ACTION == 'apply' && params.DESTROY_ALL) { echo 'DESTROY_ALL is ignored for ACTION=apply' }
          env.CHANGE_REQUEST = ''
          setDescription()
        }
      }
    }

    stage('Change request (prod)') {
      when { expression { params.TERRAFORM_WORKSPACE == 'prod' } }
      steps {
        script {
          notifyEmail('INPUT NEEDED: change request number',
            "A ${params.ACTION} of PROD is waiting for its change request number (10 minutes):\n${env.BUILD_URL}input/")
          timeout(time: 10, unit: 'MINUTES') {
            String cr = input(approvalArgs(
              id: 'ChangeRequest',
              message: "PROD ${params.ACTION}: enter the approved change request number",
              ok: 'Continue',
              parameters: [string(name: 'CHANGE_REQUEST', defaultValue: '', description: 'e.g. CHG0012345')]
            ))
            cr = (cr ?: '').trim()
            if (!(cr ==~ /[A-Za-z0-9][A-Za-z0-9_-]{2,31}/)) {
              error("'${cr}' is not a change request number (3-32 letters, digits, _ or -)")
            }
            env.CHANGE_REQUEST = cr
          }
          setDescription()
          echo "Change request: ${env.CHANGE_REQUEST}"
        }
      }
    }

    stage('Checkout') {
      steps {
        checkout([$class: 'GitSCM',
          branches: [[name: params.BRANCH]],
          userRemoteConfigs: scm.userRemoteConfigs,
          extensions: [[$class: 'CleanBeforeCheckout']]])
        script { env.GIT_COMMIT_SHORT = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim() }
        echo "Checked out ${params.BRANCH} @ ${env.GIT_COMMIT_SHORT}"
      }
    }

    stage('Terraform init + workspace') {
      steps {
        script {
          awsSession {
            sh '''
              set -eu
              test -f "backend/${TF_WS}.s3.tfbackend" || { echo "Missing backend/${TF_WS}.s3.tfbackend"; exit 1; }
              test -f "tfvars/${TF_WS}.tfvars"        || { echo "Missing tfvars/${TF_WS}.tfvars"; exit 1; }
              terraform version
              aws sts get-caller-identity --query Arn --output text
              terraform init -reconfigure -backend-config="backend/${TF_WS}.s3.tfbackend"
              terraform workspace select -or-create "${TF_WS}"
              test "$(terraform workspace show)" = "${TF_WS}"
              terraform fmt -check -recursive || echo "WARNING: files are not terraform-fmt formatted"
              terraform validate
            '''
          }
        }
      }
    }

    stage('Plan') {
      steps {
        script {
          awsSession {
            if (params.ACTION == 'apply') {
              sh 'terraform plan $TF_ARGS -out=tfplan'
            } else {
              failIfGlueRunning()
              sh 'terraform plan -destroy $TF_ARGS -out=tfplan'
            }
            sh 'terraform show -no-color tfplan > tfplan.txt'
          }
          env.PLAN_SUMMARY = sh(returnStdout: true,
            script: 'grep -E "^(Plan:|No changes\\.|Changes to Outputs:)" tfplan.txt | head -1 || true').trim() ?: 'see tfplan.txt'
          echo "Plan for ${params.TERRAFORM_WORKSPACE}: ${env.PLAN_SUMMARY}"
          archiveArtifacts artifacts: 'tfplan.txt', allowEmptyArchive: false
        }
      }
    }

    stage('Approval') {
      steps {
        script {
          String what = params.ACTION == 'destroy'
            ? "DESTROY EVERYTHING in workspace '${params.TERRAFORM_WORKSPACE}' (incl. the job bucket and all reports)"
            : "APPLY to workspace '${params.TERRAFORM_WORKSPACE}'" + (params.COPY_SCRIPTS ? ' and copy the scripts' : '')
          String details = "${what}\nPlan: ${env.PLAN_SUMMARY}\nBranch: ${params.BRANCH} @ ${env.GIT_COMMIT_SHORT}" +
            (env.CHANGE_REQUEST ? "\nChange request: ${env.CHANGE_REQUEST}" : '') +
            "\nPlan file: ${env.BUILD_URL}artifact/tfplan.txt"
          notifyEmail("APPROVAL NEEDED: ${params.ACTION} ${params.TERRAFORM_WORKSPACE}",
            "${details}\n\nApprove or abort within 10 minutes: ${env.BUILD_URL}input/")
          timeout(time: 10, unit: 'MINUTES') {
            input(approvalArgs(
              id: 'ConfirmChange',
              message: "${details}\n\nConfirm this change?",
              ok: params.ACTION == 'destroy' ? "Destroy ${params.TERRAFORM_WORKSPACE}" : "Apply to ${params.TERRAFORM_WORKSPACE}"
            ))
          }
          echo "Approved: ${what}"
        }
      }
    }

    stage('Apply / Destroy') {
      steps {
        script {
          awsSession {
            if (params.ACTION == 'apply') {
              sh 'terraform apply tfplan'
            } else {
              failIfGlueRunning() // again: a run may have started while waiting for approval
              // The job bucket is versioned and holds objects: let Terraform empty it.
              // This targeted apply only flips force_destroy on the bucket.
              int bucketManaged = sh(returnStatus: true,
                script: 'terraform state list | grep -q "^module.job_bucket.aws_s3_bucket.this$"')
              if (bucketManaged == 0) {
                sh 'terraform apply -auto-approve $TF_ARGS -var=job_bucket_force_destroy=true -target=module.job_bucket'
              }
              sh '''
                terraform plan -destroy $TF_ARGS -var=job_bucket_force_destroy=true -out=tfplan-destroy
                terraform apply tfplan-destroy
              '''
            }
          }
        }
      }
    }

    stage('Copy scripts to S3') {
      when { expression { params.ACTION == 'apply' } }
      steps {
        script {
          awsSession {
            def out = terraformOutputs()
            withEnv(["SCRIPTS_PREFIX=${out.scripts_prefix.value}"]) {
              boolean missing = sh(returnStatus: true,
                script: 'aws s3 ls "${SCRIPTS_PREFIX}find.py" >/dev/null 2>&1') != 0
              if (!params.COPY_SCRIPTS && !missing) {
                echo "COPY_SCRIPTS not ticked: the scripts in ${env.SCRIPTS_PREFIX} are left unchanged"
                return
              }
              if (missing && !params.COPY_SCRIPTS) { echo 'No scripts in the bucket yet (first deployment): copying them' }
              failIfGlueRunning()
              sh '''
                set -eu
                aws s3 sync scripts/ "$SCRIPTS_PREFIX" \
                  --exclude "*" --include "*.py" --exclude "__pycache__/*" --only-show-errors
                echo "Scripts now in $SCRIPTS_PREFIX:"
                aws s3 ls "$SCRIPTS_PREFIX"
              '''
              // Every module the jobs import must be there (see locals.tf shared_modules).
              ['find', 'forget', 'arguments', 'common', 'formats', 'tasks',
               'find_strategies', 'forget_strategies', 'tracking', 'notify'].each { name ->
                withEnv(["SCRIPT_NAME=${name}"]) {
                  sh 'aws s3 ls "${SCRIPTS_PREFIX}${SCRIPT_NAME}.py" >/dev/null'
                }
              }
            }
          }
        }
      }
    }

    stage('Outputs') {
      when { expression { params.ACTION == 'apply' } }
      steps {
        script {
          awsSession {
            def out = terraformOutputs()
            env.RESULT_TEXT = """Environment ${out.environment.value}: job bucket ${out.job_bucket.value}
Set these defaults in the find/status/forget/stop Jenkins jobs:
  GLUE_JOB / FIND_JOB = ${out.job_names.value.find}
  FORGET_JOB          = ${out.job_names.value.forget}
  RESULT_PREFIX       = ${out.result_prefix.value}
  STOP_URI            = ${out.stop_uri.value}
  AWS_REGION          = ${out.region.value}
Role ARNs for the archive bucket / KMS policies (GUIDE 7.5): ${out.role_arns.value}"""
            echo env.RESULT_TEXT
          }
        }
      }
    }
  }

  post {
    always {
      archiveArtifacts artifacts: 'tfplan.txt, terraform-outputs.json', allowEmptyArchive: true
    }
    success {
      notifyEmail("SUCCESS: ${params.ACTION} ${params.TERRAFORM_WORKSPACE}",
        "Plan: ${env.PLAN_SUMMARY ?: '-'}\n\n${env.RESULT_TEXT ?: ''}\n\n${env.BUILD_URL}")
    }
    failure {
      notifyEmail("FAILED: ${params.ACTION} ${params.TERRAFORM_WORKSPACE}",
        "The provisioning build failed. Console: ${env.BUILD_URL}console")
    }
    aborted {
      notifyEmail("ABORTED: ${params.ACTION} ${params.TERRAFORM_WORKSPACE}",
        "Not approved within 10 minutes, rejected, or stopped. Nothing was applied after the approval step.\n${env.BUILD_URL}")
    }
  }
}

// ---------------------------------------------------------------- helpers

// Every AWS and Terraform operation runs inside this closure: it assumes
// ROLE_NAME in ROLE_ACCOUNT with EXTERNAL_ID, in AWS_REGION, and exposes the
// workspace and the common Terraform arguments to the shell.
def awsSession(Closure body) {
  withAWS(role: env.ROLE_NAME,
          roleAccount: env.ROLE_ACCOUNT,
          externalId: env.EXTERNAL_ID,
          region: env.AWS_REGION,
          roleSessionName: "drop-glue-provision-${env.BUILD_NUMBER}",
          duration: 3600) {
    withEnv([
      "TF_WS=${params.TERRAFORM_WORKSPACE}",
      "AWS_DEFAULT_REGION=${env.AWS_REGION}",
      // tfvars of the workspace; the region and the notification address come from Jenkins
      "TF_ARGS=-var-file=tfvars/${params.TERRAFORM_WORKSPACE}.tfvars -var=region=${env.AWS_REGION} -var=notify_to=${env.NOTIFICATION_EMAIL.replaceAll(/\s/, '')}",
    ]) {
      body()
    }
  }
}

// Terraform outputs of the current workspace (also saved as a build artifact).
// Call inside awsSession: the state is read from S3.
def terraformOutputs() {
  sh 'terraform output -json > terraform-outputs.json'
  return readJSON(file: 'terraform-outputs.json')
}

// Refuses to continue while a Find/Forget run of this workspace is active:
// destroying, or replacing the scripts under a running job, would break the batch.
// Call inside awsSession.
def failIfGlueRunning() {
  def out = terraformOutputs()
  if (!out.job_names) {
    echo 'No Glue jobs in this workspace yet'
    return
  }
  out.job_names.value.each { phase, jobName ->
    withEnv(["GLUE_JOB_NAME=${jobName}"]) {
      String active = sh(returnStdout: true, script: '''
        aws glue get-job-runs --job-name "$GLUE_JOB_NAME" --max-results 50 \
          --query "length(JobRuns[?JobRunState=='STARTING' || JobRunState=='RUNNING' || JobRunState=='STOPPING' || JobRunState=='WAITING'])" \
          --output text 2>/dev/null || echo 0
      ''').trim()
      if (active != '0' && active != 'None') {
        error("Glue job ${jobName} has ${active} active run(s). Wait for it, or stop it with drop-glue-stop, then rebuild.")
      }
    }
  }
}

// input() arguments, restricted to APPROVERS when that variable is set.
def approvalArgs(Map args) {
  if (env.APPROVERS?.trim()) { args.submitter = env.APPROVERS.trim() }
  return args
}

def setDescription() {
  currentBuild.description = "${params.ACTION} ${params.TERRAFORM_WORKSPACE} @ ${params.BRANCH}" +
    (params.COPY_SCRIPTS ? ' + scripts' : '') +
    (env.CHANGE_REQUEST ? " | CR ${env.CHANGE_REQUEST}" : '')
}

// Never fails the build because of mail problems.
def notifyEmail(String subject, String body) {
  if (!env.NOTIFICATION_EMAIL?.trim()) { return }
  try {
    mail(to: env.NOTIFICATION_EMAIL,
         subject: "[drop-glue provision] ${subject}" + (env.CHANGE_REQUEST ? " (CR ${env.CHANGE_REQUEST})" : ''),
         body: "${body}\n\nJob: ${env.JOB_NAME} #${env.BUILD_NUMBER}\nWorkspace: ${params.TERRAFORM_WORKSPACE}, branch: ${params.BRANCH}")
  } catch (err) {
    echo "WARNING: notification email not sent: ${err}"
  }
}
