import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("file")


class DeleteS3Objects:
    """Manages S3 objects."""

    def __init__(self, bucket: str):
        self.bucket = bucket

        s3_resource = boto3.resource("s3")
        self.client = s3_resource.Bucket(self.bucket)

    def delete_objects(self, object_keys: list):
        """
        Removes a list of objects from a bucket.
        This operation is done as a batch in a single request.

        :param bucket: The bucket that contains the objects. This is a Boto3 Bucket
                       resource.
        :param object_keys: The list of keys that identify the objects to remove.
        :return: The response that contains data about which objects were deleted
                 and any that could not be deleted.
        """
        try:
            response = self.client.delete_objects(
                Delete={"Objects": [{"Key": key} for key in object_keys]}
            )
            if "Deleted" in response:
                logger.info(
                    "Deleted objects '%s' from bucket '%s'.",
                    [del_obj["Key"] for del_obj in response["Deleted"]],
                    self.client.name,
                )
            if "Errors" in response:
                logger.warning(
                    "Could not delete objects '%s' from bucket '%s'.",
                    [
                        f"{del_obj['Key']}: {del_obj['Code']}"
                        for del_obj in response["Errors"]
                    ],
                    self.client.name,
                )
        except ClientError:
            logger.exception(
                "Couldn't delete any objects from bucket %s.", self.client.name
            )
            raise
        else:
            return response
