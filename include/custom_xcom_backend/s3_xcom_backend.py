from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import uuid
import os


class CustomXComBackendS3(BaseXCom):
    # the prefix is optional and used to make it easier to recognize
    # which reference strings in the Airflow metadata database
    # refer to an XCom that has been stored in an S3 bucket
    PREFIX = "xcom_s3://"
    BUCKET_NAME = os.environ["XCOM_BACKEND_BUCKET_NAME"]
    AWS_CONN_ID = os.environ["XCOM_BACKEND_AWS_CONN_ID"]

    @staticmethod
    def serialize_value(
        value,
        key=None,
        task_id=None,
        dag_id=None,
        run_id=None,
        map_index=None,
        **kwargs,
    ):
        # the connection to AWS is created by using the S3 hook with
        # the conn id configured in Step 3
        hook = S3Hook(aws_conn_id=CustomXComBackendS3.AWS_CONN_ID)
        # make sure the file_id is unique, either by using combinations of
        # the task_id, run_id and map_index parameters or by using a uuid
        filename = "data_" + str(uuid.uuid4()) + ".json"
        # define the full S3 key where the file should be stored
        s3_key = f"{run_id}/{task_id}/{filename}"

        # write the value to a local temporary JSON file
        with open(filename, "a+") as f:
            json.dump(value, f)

        # load the local JSON file into the S3 bucket
        hook.load_file(
            filename=filename,
            key=s3_key,
            bucket_name=CustomXComBackendS3.BUCKET_NAME,
            replace=True,
        )

        # remove the local temporary JSON file
        os.remove(filename)

        # define the string that will be saved to the Airflow metadata
        # database to refer to this XCom
        reference_string = CustomXComBackendS3.PREFIX + s3_key

        # use JSON serialization to write the reference string to the
        # Airflow metadata database (like a regular XCom)
        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result):
        # retrieve the relevant reference string from the metadata database
        reference_string = BaseXCom.deserialize_value(result=result)

        # create the S3 connection using the S3Hook and recreate the S3 key
        hook = S3Hook(aws_conn_id=CustomXComBackendS3.AWS_CONN_ID)
        key = reference_string.replace(CustomXComBackendS3.PREFIX, "")

        # download the JSON file found at the location described by the
        # reference string to a temporary local folder
        filename = hook.download_file(
            key=key,
            bucket_name=CustomXComBackendS3.BUCKET_NAME,
            local_path="/usr/local/airflow/include",
        )

        # load the content of the local JSON file and return it to be used by
        # the operator
        with open(filename, "r") as f:
            output = json.loads(f.read())

        # remove the local temporary JSON file
        os.remove(filename)

        return output
