"""Kiểm tra kết nối tới bucket S3: ghi, liệt kê, đọc lại, rồi xoá một object nhỏ."""
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

HEALTHCHECK_PREFIX = "bronze/_healthcheck/"


def main() -> int:
    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("Thiếu biến S3_BUCKET trong file .env")
        return 1

    s3 = boto3.client("s3")
    key = f"{HEALTHCHECK_PREFIX}{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.txt"

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"ok")
        print(f"[1/4] Ghi: s3://{bucket}/{key}")

        resp = s3.list_objects_v2(Bucket=bucket, Prefix=HEALTHCHECK_PREFIX)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        if key not in keys:
            print("[2/4] Liệt kê: KHÔNG thấy object vừa ghi")
            return 1
        print(f"[2/4] Liệt kê: thấy object vừa ghi ({len(keys)} object dưới {HEALTHCHECK_PREFIX})")

        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if body != b"ok":
            print(f"[3/4] Đọc lại: nội dung sai {body!r}")
            return 1
        print("[3/4] Đọc lại: nội dung khớp")

        s3.delete_object(Bucket=bucket, Key=key)
        print("[4/4] Xoá: xong")

    except NoCredentialsError:
        print("Không tìm thấy khoá AWS. Kiểm tra AWS_ACCESS_KEY_ID và AWS_SECRET_ACCESS_KEY trong .env")
        return 1
    except ClientError as err:
        error = err.response["Error"]
        print(f"AWS từ chối: {error['Code']} - {error['Message']}")
        return 1

    print(f"Kết nối S3 tới bucket '{bucket}' hoạt động tốt.")
    return 0


if __name__ == "__main__":
    sys.exit(main())