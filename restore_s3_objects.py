#!/usr/bin/env python3

import argparse
import logging
import os
import sys

from datetime import datetime
from itertools import islice

import aws_s3.delete

logger = logging.getLogger("file")
slogger = logging.getLogger("stdout")


class RestoreObjects:
    """
    Restores S3 objects from a bucket. S3 object and version ID list is read from a provided comma-separated file.
    """

    def __init__(self, **kwargs):
        # We may not need to specify an action. Commenting it out for now.
        # self.action = None
        self.bucket = None
        self.batch_size = None
        self.data_file = None

        if "log_file" in kwargs and kwargs["log_file"] != "":
            self.log_file = kwargs["log_file"]
        else:
            now = datetime.now().strftime("%Y%d%m%H%M%S")
            self.log_file = f"logs/results-{now}.log"

        self._init_logging()

    def _init_logging(self):
        # File logger
        logger.setLevel(logging.INFO)

        log_dir = os.path.dirname(self.log_file)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir, mode=0o755)

        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        aws_s3.delete.logger.addHandler(file_handler)

        # STDOUT logger
        slogger.setLevel(logging.INFO)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)

        stdout_handler.setFormatter(formatter)

        slogger.addHandler(stdout_handler)

    def caller(
        self,
        bucket: str = "",
        data_file: str = "",
        batch_size: int = 1000,
    ):
        """
        Caller function. Parses arguments, creates iteration based on batch size, and calls
        downstream function.
        """

        if self.data_file is None:
            self.data_file = data_file
            if self.data_file == "":
                raise ValueError("'data_file' argument must not be empty")

        if self.bucket is None:
            self.bucket = bucket
            if self.bucket == "":
                raise ValueError("'bucket' argument must not be empty")

        if self.batch_size is None:
            self.batch_size = batch_size

        try:
            with open(data_file, "r+", encoding="utf8") as f:
                slogger.info("Logging to %s", self.log_file)
                logger.info("Bucket: %s", self.bucket)
                logger.info("Source file: %s", data_file)
                logger.info("Batch size: %s", batch_size)
                logger.info("Starting restoration...")

                while True:
                    next_lines = [
                        {
                            "Key": s.split(",")[0],
                            "VersionId": s.split(",")[1].replace("\n", ""),
                        }
                        for s in list(islice(f, batch_size))
                        if len(s.split(",")) == 2
                    ]
                    if not next_lines:
                        break

                    self._restore(next_lines)

                logger.info("Restoration complete.")
                slogger.info("Restoration complete.")

        except FileNotFoundError as error:
            logger.error("File %s was not found!", data_file)
            raise FileNotFoundError(f"File {data_file} was not found!") from error

    def restore(self, bucket: str = "", data_file: str = "", batch_size: int = 1000):
        """
        Restore all provided objects in a bucket using batching.
        """

        self.batch_size = batch_size
        self.bucket = bucket
        self.data_file = data_file
        self.caller()

    def _restore(self, object_list: list):
        client = aws_s3.delete.DeleteS3Objects(self.bucket)
        client.delete_object_versions(object_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="restore_s3_objects.py",
        description="Restores objects from S3 in bulk using a list of objects and version IDs in a comma-separated file",
    )

    parser.add_argument("bucket", help="Name of S3 bucket", type=str)

    parser.add_argument(
        "filename",
        help="Filename of flat file containing S3 objects to restore",
        type=str,
    )

    def check_batch_size(batch_size: int):
        """
        Verify that batch size does not exceed 1000.
        """

        batch_size = int(batch_size)
        if batch_size > 1000:
            raise argparse.ArgumentTypeError("Batch size cannot exceed 1000")

        return batch_size

    parser.add_argument(
        "-b",
        "--batch-size",
        dest="batch_size",
        help="Number of objects to restore in each batch (default: 1000, max: 1000)",
        default=1000,
        required=False,
        type=check_batch_size,
    )

    parser.add_argument(
        "-l",
        "--log-file",
        dest="log_file",
        help="File to use for logging. (default: results-{datetime}.log)",
        default="",
    )

    args = parser.parse_args()

    restore_objects = RestoreObjects(log_file=args.log_file)
    restore_objects.caller(args.bucket, args.filename, args.batch_size)
