import pandas as pd
import boto3
import json
import configparser
import psycopg2

#Reference: This code was taken from Infrastructure as a Code notebook in module 2 from Udacity Data Engineering Nanodegree
#Link: https://www.udacity.com/course/data-engineer-nanodegree--nd027



def create_iam_role(iam, dwh_iam_role_name):
    """
    Function to create IAM role
    """

    try:
        print('1.1 Creating a new IAM Role')
        dwh_role = iam.create_role(Path='/',
                                   RoleName=dwh_iam_role_name,
                                   Description="Allows Redshift clusters to call AWS services on your behalf.",
                                   AssumeRolePolicyDocument=json.dumps(
                                       {'Statement': [{'Action': 'sts:AssumeRole',
                                                       'Effect': 'Allow',
                                                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                        'Version': '2012-10-17'})
                                   )
    except Exception as e:
        print(e)
        
        
    iam.attach_role_policy(RoleName=dwh_iam_role_name,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    role_arn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']
    return role_arn


def create_redshift_cluster(redshift, roleArn, dwh_cluster_type, dwh_node_type, dwh_num_nodes, dwh_db, dwh_cluster_identifier, dwh_db_user, dwh_db_password):
    """
    Function to create redshift cluster
    """
    try:
        response = redshift.create_cluster(
            ClusterType=dwh_cluster_type,
            NodeType=dwh_node_type,
            NumberOfNodes=int(dwh_num_nodes),
            DBName=dwh_db,
            ClusterIdentifier=dwh_cluster_identifier,
            MasterUsername=dwh_db_user,
            MasterUserPassword=dwh_db_password,
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)


def pretty_print_redshift_properties(props):
    """
    Function to print redshift cluster properties in a pretty way
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def get_cluster_properties(redshift, dwh_cluster_identifier):
    """
    Function to get properties of cluster
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    return myClusterProps


def destroy_created_cluster(redshift, dwh_cluster_identifier):
    """
    Function to destroy cluster that is created
    """
    redshift.delete_cluster( ClusterIdentifier=dwh_cluster_identifier,  SkipFinalClusterSnapshot=True)
    
    
def delete_role_and_detach_policy(iam, dwh_iam_role_name):
    """
    Function to delete role and detach policy
    """
    iam.detach_role_policy(RoleName=dwh_iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=dwh_iam_role_name)


def open_ports_to_cluster_access(ec2, myClusterProps, dwh_port):
    """
    Function to open endpoint with the cluster using ec2 instance
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )

    except Exception as e:
        print(e)


def main():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    param_df = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
    
    print(param_df.head(10))

    ec2 = boto3.resource('ec2',
                         region_name='us-west-2',
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_redshift_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    df = pretty_print_redshift_properties(cluster_properties)
    print(df.head(10))

    myClusterProps = get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)

    open_ports_to_cluster_access(ec2, myClusterProps, DWH_PORT)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()


if __name__ == "__main__":
    main()



