''' Aws Script for AWS Management

This python script can be used to manage a AWS endorsement.

This provides features like :

 EC2 :

1. Create your own Key based SnapShots 
2. Delete SnapShots 
3. Delete/ Terminate any EC2 instance which does not have a user/ any specific tag 
4. stop any useless Running Ec2 instance

 RDS : 

1. delete RDS Instance
2. Delete RDS Cluster
3. Stop any useless Running RDS Cluster/Instance
4. Delete useless Snapshots 
5. Delete any RDS Instance or Cluster which does not have a specific tag with it 
6. Delete any RDS Snapshot that is older then 2 days 

'''


import boto3
import datetime


class Rds(object):

    def __init__(self, region) -> None:
        super().__init__()
        self.rds = boto3.client('rds', region)

    def cleanup_snapshot(self):
        self._cleanup_snapshot_instance()
        self._cleanup_snapshots_clusters()

    def cleanup_instances(self):
        clusters = self.rds.describe_db_clusters()
        for cluster in clusters['DBClusters']:
            self._cleanup_cluster(cluster)
        instances = self.rds.describe_db_instances()
        for instance in instances['DBInstances']:
            self._cleanup_instance(instance)

    def _stop_cluster(self, identifier):
        self.rds.stop_db_cluster(DBClusterIdentifier=identifier)

    def _stop_instance(self, identifier):
        self.rds.stop_db_instance(DBInstanceIdentifier=identifier)

    def _delete_instance(self, identifier):
        self.rds.describe_db_instances(DBInstanceIdentifier=identifier)

    def _delete_cluster(self, identifier):
        self.rds.describe_db_clusters(DBClusterIdentifier=identifier)

    def _delete_instance_snapshot(self, identifier):
        self.rds.delete_db_snapshot(DBSnapshotIdentifier=identifier)

    def _delete_cluster_snapshot(self, identifier):
        self.rds.delete_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=identifier)

    @staticmethod
    def _can_delete_instance(tags):
        if any('user' in tag for tag in tags):
            return False

    @staticmethod
    def _can_stop_instance(tags):
        for tag in tags:
            if tag["Key"].lower() == 'excludepower' and tag['Value'].lower() == 'true':
                return False
        return True

    @staticmethod
    def _can_delete_snapshot(tags):
        if tags is not None:
            for tag in tags:
                if tag['Key'].lower() == 'retain' and tag['Value'].lower() == 'true':
                    return False
        return True

    def _cleanup_instance(self, instance):
        identifier = instance['DBInstanceIdentifier']
        tags = instance['TagList']
        if self._can_delete_instance(tags):
            self._delete_instance(identifier)
        else:
            if self._can_stop_instance(tags) and instance['DBInstanceStatus'] == 'available':
                try:
                    self._stop_instance(identifier)
                except Exception as e:
                    print(str(e))

    def _cleanup_cluster(self, cluster):
        tags = cluster['TagList']
        if self._can_delete_instance(tags):
            self._delete_cluster(cluster['DBClusterIdentifier'])
        else:
            if self._can_stop_instance(tags) and cluster['Status'] == 'available':
                try:
                    self._stop_cluster(cluster['DBClusterIdentifier'])
                except Exception as e:
                    print(str(e))

    def _cleanup_snapshots_clusters(self):
        snapshots = self.rds.describe_db_cluster_snapshots()
        for snapshot in snapshots['DBClusterSnapshots']:
            tags = snapshot['TagList']
            if self._can_delete_snapshot(tags) and self._is_older_snapshot(
                    str(snapshot['SnapshotCreateTime']).split(" ")):
                try:
                    self._delete_cluster_snapshot(
                        snapshot['DBClusterSnapshotIdentifier'])
                except Exception as e:
                    print(str(e))

    def _cleanup_snapshot_instance(self):
        snapshots = self.rds.describe_db_snapshots()
        for snapshot in snapshots['DBSnapshots']:
            tags = snapshot['TagList']
            if self._can_delete_snapshot(tags) and self._is_older_snapshot(
                    str(snapshot['SnapshotCreateTime']).split(" ")):
                try:
                    self._delete_instance_snapshot(
                        snapshot['DBSnapshotIdentifier'])
                except Exception as e:
                    print(str(e))

    @staticmethod
    def _is_older_snapshot(snapshot_datetime):
        snapshot_date = snapshot_datetime[0].split("-")
        snapshot_date = datetime.date(int(snapshot_date[0]), int(
            snapshot_date[1]), int(snapshot_date[2]))
        today = datetime.date.today()
        if abs(today - snapshot_date).days > 2:
            return True
        else:
            return False

    @staticmethod
    def _check_snapshot_tag(tags):
        flag = False
        for tag in tags:
            if tag['Key'].lower() == 'retain' and tag['Value'].lower() == 'true':
                flag = True
        if flag:
            return True
        else:
            return False


if __name__ == "__main__":
    rds = Rds('us-east-1')
    #     # rds.shutdown()
    rds.cleanup_snapshot()
    rds.cleanup_instances()

    from datetime import datetime, timedelta, timezone

import boto3


def get_delete_data(older_days):
    delete_time = datetime.now(tz=timezone.utc) - timedelta(days=older_days)
    return delete_time


def is_ignore_shutdown(tags):
    for tag in tags:
        print("K " + str(tag['Key']) + " is " + str(tag['Value']))
        if str(tag['Key']) == 'excludepower' and str(tag['Value']) == 'true':
            print("Not stopping K " +
                  str(tag['Key']) + " is " + str(tag['Value']))
            return True
    return False


def is_unassigned(tags):
    if 'user' not in [t['Key'] for t in tags]:
        return True
    return False


class Ec2Instances(object):

    def __init__(self, region):
        print("region " + region)

        # if you are not using AWS Tool Kit tool you will be needing to pass your access key and secret key here

        # client = boto3.client('rds', region_name=region_name, aws_access_key_id=aws_access_key_id,
        #                       aws_secret_access_key=aws_secret_access_key)
        self.ec2 = boto3.client('ec2', region_name=region)

    def delete_snapshots(self, older_days=2):
        delete_snapshots_num = 0
        snapshots = self.get_nimesa_created_snapshots()
        for snapshot in snapshots['Snapshots']:
            fmt_start_time = snapshot['StartTime']
            if fmt_start_time < get_delete_data(older_days):
                try:
                    self.delete_snapshot(snapshot['SnapshotId'])
                    delete_snapshots_num + 1
                except Exception as e:
                    print(e)
        return delete_snapshots_num

    def get_user_created_snapshots(self):
        snapshots = self.ec2.describe_snapshots(
            Filters=[{
                'Name': 'owner-id', 'Values': ['your owner id'],
            }])  # Filters=[{'Name': 'description', 'Values': ['Created by Nimesa']}]
        return snapshots

    def delete_available_volumes(self):
        volumes = self.ec2.describe_volumes()['Volumes']
        for volume in volumes:
            if volume['State'] == "available":
                self.ec2.delete_volume(VolumeId=volume['VolumeId'])

    def delete_snapshot(self, snapshot_id):
        self.ec2.delete_snapshot(SnapshotId=snapshot_id)

    def shutdown(self):
        instances = self.ec2.describe_instances()
        instance_to_stop = []
        instance_to_terminate = []
        for res in instances['Reservations']:
            for instance in res['Instances']:
                tags = instance.get('Tags')
                if tags is None:
                    instance_to_terminate.append(instance['InstanceId'])
                    continue
                if is_unassigned(tags):
                    print("instance_to_terminate " + instance['InstanceId'])
                    instance_to_terminate.append(instance['InstanceId'])
                if is_ignore_shutdown(tags):
                    continue
                if instance['State']['Code'] == 16:
                    instance_to_stop.append(instance['InstanceId'])

        if any(instance_to_stop):
            self.ec2.stop_instances(
                InstanceIds=instance_to_stop
            )
        if any(instance_to_terminate):
            print(instance_to_terminate)
            self.ec2.terminate_instances(
                InstanceIds=instance_to_terminate
            )


def lambda_handler(event, context):
    print("event " + str(event))
    print("context " + str(context))
    ec2_reg = boto3.client('ec2')
    regions = ec2_reg.describe_regions()
    for region in regions['Regions']:
        region_name = region['RegionName']
        instances = Ec2Instances(region_name)
        deleted_counts = instances.delete_snapshots(1)
        instances.delete_available_volumes()
        print("deleted_counts for region " +
              str(region_name) + " is " + str(deleted_counts))
        instances.shutdown()
        print("For RDS")
        rds = Rds(region_name)
        rds.cleanup_snapshot()
        rds.cleanup_instances()
    return 'Hello from Lambda'




