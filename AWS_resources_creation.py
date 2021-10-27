import boto3
import json
import configparser
import pandas as pd
import time
import psycopg2

from settings import KEY, SECRET, SPARKIFY_DB_PASSWORD

# Load DWH Params from dwh.cfg file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_CLUSTER_TYPE = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("CLUSTER", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("CLUSTER", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

DWH_IAM_ROLE_NAME = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

# Print params
print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB",
                   "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                   SPARKIFY_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             }))

# Create clients for IAM, EC2, S3 and Redshift
ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB",
                   "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                   SPARKIFY_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             }))

# IAM role creation that makes Redshift able to access S3 buckets only
try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument= json.dumps(
        {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal':{'Service':'redshift.amazonaws.com'}}],
         'Version': '2012-10-17'}
        ))
except Exception as e:
    print(e)

print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(f"rol ARN: {roleArn}")


# CREATE CLUSTER WITH
try:
    response = redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=SPARKIFY_DB_PASSWORD,

        # Roles (for s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))

time.sleep(60*3)  # Wait until cluster is available
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
print(f"sparkify_DB_endpoint: {DWH_ENDPOINT}")

# Open an incoming TCP port to access the cluster ednpoint
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)


conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, SPARKIFY_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
print(conn_string)

# Connect to Database
connection = psycopg2.connect(
    dbname=DWH_DB,
    user=DWH_DB_USER,
    password=SPARKIFY_DB_PASSWORD,
    host=DWH_ENDPOINT,
    port=DWH_PORT
)
cur = connection.cursor()
print(cur.execute("SELECT 1"))
