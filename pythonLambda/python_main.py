from urllib.parse import unquote_plus
import boto3
from io import BytesIO
import gzip

fixed_widths = {
            "date": [0, 15],
            "serial_number": [15, 36],
            "model": [36, 79],
            "capacity_bytes": [79, 98],
            "failure": [98, 109]
}


def download(s3_client: object, bucket: str, key: str) -> BytesIO:
    file_object = BytesIO()
    s3_client.download_fileobj(bucket, key, file_object)
    file_object.seek(0)
    return file_object


def read_mem_file(fo: BytesIO) -> list:
    with gzip.open(fo, mode="rt") as f:
        rows = f.readlines()
    return rows


def convert_row_to_tab(raw_row: str, meta: dict) -> str:
    row = ''
    for k, v in meta.items():
        column_value = raw_row[v[0]:v[1]]
        row += column_value.strip() + '\t'
    return row + '\n'


def rows_to_file_object_gz(rws: list):
    with gzip.open('/tmp/file.gz', 'wt') as f:
        f.writelines(rws)


def file_object_to_s3(s3_client: object, bucket: str, key: str) -> None:
    s3_client.upload_file('/tmp/file.gz', bucket,
                          key.replace('.gz', '_tab.gz').replace('fixed_width_raw', 'tab_converted'))


def lambda_handler(event, _):
    s3_client = boto3.client('s3')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        fo = download(s3_client, bucket, key)
        data = read_mem_file(fo)
        tab_rows = []
        for row in data:
            tab_row = convert_row_to_tab(raw_row=row, meta=fixed_widths)
            tab_rows.append(tab_row)
        rows_to_file_object_gz(tab_rows)
        file_object_to_s3(s3_client, bucket, key)
