import asyncio
import aiofiles
from aiobotocore.session import get_session
from contextlib import asynccontextmanager
import gzip
import os
from datetime import datetime

class S3Logger:
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str):
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url,
        }
        self.bucket_name = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client("s3", **self.config) as client:
            yield client

    async def upload_log(self, log_path: str):
        object_name = self._generate_s3_object_name(log_path)
        async with self.get_client() as client:
            try:
                async with aiofiles.open(log_path, "rb") as log_file:
                    compressed_data = await self._compress_data(await log_file.read())
                    await client.put_object(
                        Bucket=self.bucket_name,
                        Key=object_name,
                        Body=compressed_data,
                        ContentEncoding='gzip'
                    )
                print(f"Log '{object_name}' successfully uploaded to S3.")
            except Exception as e:
                print(f"Failed to upload log '{object_name}': {e}")

    def _generate_s3_object_name(self, log_path: str) -> str:
        date_str = datetime.now().strftime('%Y/%m/%d')
        log_filename = os.path.basename(log_path)
        return f"logs/{date_str}/{log_filename}.gz"

    async def _compress_data(self, data: bytes) -> bytes:
        return gzip.compress(data)

async def main():
    s3_logger = S3Logger(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url="your-endpoint-url",
        bucket_name="your-bucket-name",
    )

    await s3_logger.upload_log("path_to_your_log_file.log")

if __name__ == "__main__":
    asyncio.run(main())