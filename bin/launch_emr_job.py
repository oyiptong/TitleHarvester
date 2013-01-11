#!/usr/bin/env python
# some code lifted from: https://github.com/commoncrawl/commoncrawl-crawler/blob/master/bin/launch_emr_parse_job.py
import getopt
import sys
import time

import boto
import boto.emr
from boto.emr.step import JarStep
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
from boto import Config as BotoConfig

usage_string = """
USAGE:

    launch_emr_job.py --awsKey=[awsKey] --awsSecret=[awsSecret] --keypair=[key pair name] --s3Bucket --core-count=[core count] --spot-count=[spot count] --spot-bid=[bid price] --jobAwsKey=[jobAwsKey] --jobAwsSecret=[jobAwsSecret]

Where:
    awsKey         - the aws key
    awsSecret      - the aws secret
    s3Bucket       - the s3 bucket to write results to
    keypair        - the aws public/private key pair to use 
    jobAwsKey      - aws key for job to access S3
    jobAwsSecret   - aws secret for job to access S3
    core-count     - the number of core instances to run 
    spot-count     - the number of spot instances to run 
    spot-bid       - spot instance bid price
"""

def usage():
    print usage_string
    sys.exit()

if __name__ == '__main__':
    boto_config = BotoConfig()

    try:
        opts, args = getopt.getopt(sys.argv[1:],'',['awsKey=','awsSecret=','jobAwsKey=','jobAwsSecret=','s3Bucket=','core-count=','spot-count=','spot-bid=','keypair=','test'])
    except:
        usage()

    # set your aws keys and S3 bucket, e.g. from environment or .boto

    params = {'aws_key' : None or boto_config.get('Credentials', 'aws_access_key_id'),
              'secret' : None or boto_config.get('Credentials', 'aws_secret_access_key'),
              'job_aws_key' : None or boto_config.get('Credentials', 'aws_access_key_id'),
              'job_secret' : None or boto_config.get('Credentials', 'aws_secret_access_key'),
              'keypair' : None,
              's3_bucket' : None,
              'num_core' : 2,
              'num_spot' : 0,
              'spot_bid_price' : None,
              'test_mode' : False
             }

    for o, a in opts:
        if o in ('--awsKey'):
            params['aws_key']=a
        elif o in ('--awsSecret'):
            params['secret']=a
        elif o in ('--jobAwsKey'):
            params['job_aws_key']=a
        elif o in ('--jobAwsSecret'):
            params['job_secret']=a
        elif o in ('--core-count'):
            params['num_core'] =a
        elif o in ('--spot-count'):
            params['num_spot']=a
        elif o in ('--keypair'):
            params['keypair']=a
        elif o in ('--s3Bucket'):
            params['s3_bucket']=a
        elif o in ('--spot-bid'):
            params['spot_bid_price']=a
        elif o in ('--test'):
            params['test_mode']=True

    required = ['aws_key','secret','job_aws_key','job_secret','keypair','s3_bucket']

    for pname in required:
        if not params.get(pname, None):
            print '\nERROR:%s is required' % pname
            usage()

    for p, v in params.iteritems():
        print "param:" + `p`+ " value:" + `v`

    namenode_instance_group = InstanceGroup(1,"MASTER","cc1.4xlarge","ON_DEMAND","MASTER_GROUP")
    core_instance_group = InstanceGroup(params['num_core'],"CORE","cc2.8xlarge","ON_DEMAND","CORE_GROUP")

    instance_groups=[]
    if params['num_spot'] <= 0:
        instance_groups=[namenode_instance_group,core_instance_group]
    else:
        if not params['spot_bid_price']:
            print '\nERROR:You must specify a spot bid price to use spot instances!'
            usage()
        instance_groups = [
                    namenode_instance_group,
                    core_instance_group,
                    InstanceGroup(params['num_spot'],"TASK","cc1.4xlarge","SPOT","INITIAL_TASK_GROUP",params['spot_bid_price'])
        ]

    bootstrap_step1 = BootstrapAction(
            "configure_hadoop",
            "s3://elasticmapreduce/bootstrap-actions/configure-hadoop",
            [
                "-m","mapred.tasktracker.map.tasks.maximum=32",
                "-m","mapred.child.java.opts=-XX:ErrorFile=/tmp/hs_err_${mapred.tip.id}.log -Xmx1024m -XX:+UseParNewGC -XX:ParallelGCThreads=32 -XX:NewSize=100m -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:+CMSIncrementalMode -XX:+CMSIncrementalPacing -XX:CMSIncrementalDutyCycleMin=0 -XX:CMSIncrementalDutyCycle=10",
                "-m","io.sort.factor=32",
                "-m","io.sort.mb=320",
                "-m","io.file.buffer.size=65536",
                "-m","mapred.job.tracker.handler.count=64",
            ]
    )

    bootstrap_step2 = BootstrapAction(
            "configure_jobtrackerheap",
            "s3://elasticmapreduce/bootstrap-actions/configure-daemons",
            [
                "--tasktracker-heap-size=2048"
            ]
    )

    step_args = [
        "org.mozilla.up.TitleHarvester",
        "-Dfs.s3n.awsAccessKeyId={0}".format(params['job_aws_key']),
        "-Dfs.s3n.awsSecretAccessKey={0}".format(params['job_secret']),
        "-Dfs.s3.awsAccessKeyId={0}".format(params['job_aws_key']),
        "-Dfs.s3.awsSecretAccessKey={0}".format(params['job_secret']),
        "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt",
        "s3n://{0}/valid-segments-results/".format(params['s3_bucket'])
    ]

    if params['test_mode'] == True:
        step_args.append('--testMode')

    step = JarStep(name='org.mozilla.up.TitleHarvester',
            jar='s3n://mozilla-up/TitleHarvester.jar',
            step_args=step_args
    )

    print "instance groups: ", instance_groups

    # given that a cc2.8xlarge costs $2.40/h
    # the job should cost just under $200 and run for 3 hours and a bit
    emr = boto.connect_emr()
    job_id = emr.run_jobflow(name='TitleHarvester - valid_segments',
            ec2_keyname=params['keypair'],
            log_uri='s3n://{0}/valid-segments-logs/'.format(params['s3_bucket']),
            enable_debugging=True,
            ami_version='2.3.1',
            instance_groups=instance_groups,
            steps=[step],
            bootstrap_actions=[bootstrap_step1, bootstrap_step2]
    )
    state = emr.describe_jobflow(job_id).state

    print "finished spawning job (note: starting still takes time)"
    print "job state = ", state
    print "job id = ", job_id
