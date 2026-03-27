import boto3
import boto3.session
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

KiB = 1024
MiB = 1024 * KiB

# Since we're streaming the final total size is unknown, so we have
# to tell boto3 what part size to use to accommodate the entire
# file - S3 has a hard coded limit of 10000 parts In this example
# we choose a part size of 200MB, so 2TB maximum final object size
S3_MULTIPART_CHUNK_SIZE = 200 * MiB


class S3AccessException(Exception):
    pass


class S3Client(object):
    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        endpoint_url,
        region_name,
        bucket,
        multipart_chunk_size=S3_MULTIPART_CHUNK_SIZE,
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.bucket = bucket
        self.session = boto3.session.Session()
        self.chunk_size = multipart_chunk_size
        self.config = TransferConfig(
            multipart_chunksize=multipart_chunk_size,
            multipart_threshold=multipart_chunk_size,
        )

    def get_client(self):
        return self.session.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
        )

    def upload_fileh(self, objectkey, fileh, callback=None):
        client = self.get_client()

        res = client.upload_fileobj(
            fileh,
            self.bucket,
            objectkey,
            Config=self.config,
            Callback=callback,
        )
        return res

    def download_fileh(self, objectkey, fileh, callback=None):
        client = self.get_client()
        res = client.download_fileobj(
            self.bucket, objectkey, fileh, Config=self.config, Callback=callback
        )
        return res

    def get_object_head(self, objectkey):
        client = self.get_client()
        try:
            response = client.head_object(
                Bucket=self.bucket,
                Key=objectkey,
            )
            return {
                "size": response["ContentLength"],
                "mtime": response["LastModified"],
                "etag": response["ETag"],
            }
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 403:
                raise S3AccessException from err
            else:
                raise err

    def list_objects(self, prefix=None):
        client = self.get_client()
        paginator = client.get_paginator("list_objects_v2")
        response_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
            FetchOwner=True | False,
        )
        for item_data in response_iterator:
            yield item_data

    def test_access_upload(self, object_key):
        """
        Tests if a multipart upload can be initiated and aborted.
        Returns True if successful, False otherwise.
        """
        s3_client = self.get_client()
        upload_id = None

        try:
            response = s3_client.create_multipart_upload(
                Bucket=self.bucket, Key=object_key
            )
            upload_id = response["UploadId"]

            s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=object_key, UploadId=upload_id
            )
            return True

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_msg = e.response["Error"]["Message"]

            if error_code == "AccessDenied":
                help_msg = "Error: Access Denied. Check S3 permissions."
            elif error_code == "NoSuchBucket":
                help_msg = "Error: The specified bucket does not exist."
            else:
                help_msg = f"Error: {error_code} - {error_msg}"

            # Attempt to clean up if we somehow failed after getting an upload_id
            if upload_id:
                try:
                    s3_client.abort_multipart_upload(
                        Bucket=self.bucket, Key=object_key, UploadId=upload_id
                    )
                except Exception:
                    pass
            raise S3AccessException(help_msg) from e
