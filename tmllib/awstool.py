import boto3

class AWSTool:
    def __init__(self):
        pass

    def init_s3(self, region_name='ap-northeast-1'):
        self.s3 = boto3.resource('s3', region_name=region_name)
        return self

    def get_from_s3url(self, s3url):
        p = s3url.split('/')
        bucket, key = p[2], '/'.join(p[3:])
        binary = self.s3.Bucket(bucket).Object(key).get()['Body'].read()
        return binary

