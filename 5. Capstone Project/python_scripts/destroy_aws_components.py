import configparser
import boto3
from create_aws_components import destroy_created_cluster, delete_role_and_detach_policy, pretty_print_redshift_properties


def main():
    config = configparser.ConfigParser()
    config.read_file(open('api.cfg'))

    KEY = config.get('AWS', 'ACCESS KEY')
    SECRET = config.get('AWS', 'ACCESS SECRET')

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    iam = boto3.client('iam',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    destroy_created_cluster(redshift, DWH_CLUSTER_IDENTIFIER)

    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    df = pretty_print_redshift_properties(cluster_properties)
    print(df.head(10))

    delete_role_and_detach_policy(iam, DWH_IAM_ROLE_NAME)

    print("Done!")


if __name__ == "__main__":
    main()