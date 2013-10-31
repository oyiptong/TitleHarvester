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

    launch_emr_job.py --jobName=[job name] --awsKey=[awsKey] --awsSecret=[awsSecret] --keypair=[key pair name] --s3Bucket=[bucket name] --core-count=[core count] --spot-count=[spot count] --spot-bid=[bid price] --jobAwsKey=[jobAwsKey] --jobAwsSecret=[jobAwsSecret] --availabilityZone=[availabilityZone] --data-region-code=[dataRegionCode] --data-variation=[dataVariation] --keep-alive

Where:
    awsKey              - the aws key
    awsSecret           - the aws secret
    jobName             - the name to call the job
    s3Bucket            - the s3 bucket to write results to
    keypair             - the aws public/private key pair to use 
    dataRegionCode      - region code for the data to use [optional]
    dataVariation       - the variation of data to use [optional]
    jobAwsKey           - aws key for job to access S3 [optional]
    jobAwsSecret        - aws secret for job to access S3 [optional]
    core-count          - the number of core instances to run [optional]
    spot-count          - the number of spot instances to run [optional]
    spot-bid            - spot instance bid price [optional]
    availabilityZone    - availability zone to launch EMR jobs from [optional]
    keep-alive          - keep servers around after job [optional]
"""

def usage():
    print usage_string
    sys.exit()

if __name__ == '__main__':
    boto_config = BotoConfig()

    try:
        opts, args = getopt.getopt(sys.argv[1:],'', ['awsKey=','awsSecret=','jobAwsKey=','jobAwsSecret=','s3Bucket=','core-count=','spot-count=','spot-bid=','keypair=','jobName=','data-region-code=','data-variation=','availabilityZone=','keep-alive'])
    except:
        usage()

    # set your aws keys and S3 bucket, e.g. from environment or .boto

    params = {'aws_key' : None or boto_config.get('Credentials', 'aws_access_key_id'),
              'secret' : None or boto_config.get('Credentials', 'aws_secret_access_key'),
              'job_aws_key' : None or boto_config.get('Credentials', 'aws_access_key_id'),
              'job_aws_secret' : None or boto_config.get('Credentials', 'aws_secret_access_key'),
              'keypair' : None,
              's3_bucket' : None,
              'job_name' : None,
              'data_region_code' : None,
              'data_variation' : None,
              'availability_zone' : "us-east-1d",
              'num_core' : 2,
              'num_spot' : 0,
              'spot_bid_price' : None,
              'keep_alive_mode' : False
             }

    for o, a in opts:
        if o in ('--awsKey'):
            params['aws_key']=a
        elif o in ('--awsSecret'):
            params['secret']=a
        elif o in ('--jobAwsKey'):
            params['job_aws_key']=a
        elif o in ('--jobAwsSecret'):
            params['job_aws_secret']=a
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
        elif o in ('--keep-alive'):
            params['keep_alive_mode']=True
        elif o in ('--availabilityZone'):
            params['availability_zone']=a
        elif o in ('--jobName'):
            params['job_name']=a
        elif o in ('--data-region-code'):
            params['data_region_code']=a
        elif o in ('--data-variation'):
            params['data_variation']=a

    required = ['aws_key', 'secret', 'keypair', 's3_bucket', 'job_name']

    if not (params['job_aws_key'] and params['job_aws_secret']):
        params['job_aws_key'] = params['aws_key']
        params['job_aws_secret'] = params['secret']

    for pname in required:
        if not params.get(pname, None):
            print '\nERROR:%s is required' % pname
            usage()

    for p, v in params.iteritems():
        print "param:" + `p`+ " value:" + `v`

    namenode_instance_group = InstanceGroup(1,"MASTER","m2.4xlarge","ON_DEMAND","MASTER_GROUP")
    core_instance_group = InstanceGroup(params['num_core'],"CORE","m2.4xlarge","ON_DEMAND","CORE_GROUP")

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
        "-Dfs.s3n.awsSecretAccessKey={0}".format(params['job_aws_secret']),
        "-Dfs.s3.awsAccessKeyId={0}".format(params['job_aws_key']),
        "-Dfs.s3.awsSecretAccessKey={0}".format(params['job_aws_secret']),
        "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt",
        "s3n://{0}/{1}-results/".format(params['s3_bucket'], params['job_name'])
    ]

    if params['data_region_code']:
        step_args.append("--region-code")
        step_args.append(params['data_region_code'])

    if params['data_variation']:
        step_args.append("--variation")
        step_args.append(params['data_variation'])

    step = JarStep(name='org.mozilla.up.TitleHarvester',
            jar='s3n://{0}/TitleHarvester.jar'.format(params['s3_bucket']),
            step_args=step_args
    )

    print "instance groups: ", instance_groups

    emr = boto.connect_emr()
    job_id = emr.run_jobflow(name='TitleHarvester - {0}'.format(params['job_name']),
            availability_zone=params['availability_zone'],
            ec2_keyname=params['keypair'],
            log_uri='s3n://{0}/{1}-logs/'.format(params['s3_bucket'], params['job_name']),
            enable_debugging=False,
            keep_alive=params['keep_alive_mode'],
            ami_version='2.3.1',
            instance_groups=instance_groups,
            steps=[step],
            bootstrap_actions=[bootstrap_step1, bootstrap_step2]
    )
    state = emr.describe_jobflow(job_id).state

    print "finished spawning job (note: starting still takes time)"
    print "job state = ", state
    print "job id = ", job_id
