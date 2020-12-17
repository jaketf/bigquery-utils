# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" Command Line utility for backfilling gcs_ocn_bq_ingest cloud function
"""
import argparse
import concurrent.futures
import logging
import os
import pprint
from typing import Dict, Iterator

import google.api_core.client_info
import google.api_core.exceptions
from google.cloud import storage


CLIENT_INFO = google.api_core.client_info.ClientInfo(
    user_agent="google-pso-tool/bq-severless-loader-cli")

logging.basicConfig(level='INFO')


def find_blobs_with_suffix(
    gcs_client: storage.Client,
    prefix: str,
    suffix: str = "_SUCCESS",
) -> Iterator[storage.Blob]:
    """
    Find GCS blobs with a given suffix.

    :param gcs_client:  storage.Client
    :param prefix: A GCS prefix to search i.e. gs://bucket/prefix/to/search
    :param suffix: A suffix in blob name to match
    :return:  Iterable of blobs matching the suffix.
    """
    prefix_blob: storage.Blob = storage.Blob.from_string(prefix)
    # filter passes on scalability / laziness advantages of iterator.
    return filter(
        lambda blob: blob.name.endswith(suffix),
        prefix_blob.bucket.list_blobs(client=gcs_client,
                                      prefix=prefix_blob.name))


def upload_empty_blob_and_log(gcs_client: storage.Client, blob: storage.Blob):
    try:
        blob.upload_from_string("", if_generation_match=0, client=gcs_client)
        logging.info(f"uploaded gs://{blob.bucket.name}/{blob.name}")
    except google.api_core.exceptions.PreconditionFailed:
        logging.info(
            f"skipping gs://{blob.bucket.name}/{blob.name}, already exists.")


def main(args: argparse.Namespace):
    """main entry point for backfill CLI."""
    gcs_client: storage.Client = storage.Client(client_info=CLIENT_INFO)
    suffix = args.success_filename
    repost_filename = (args.repost_filename
                       if args.repost_filename else args.success_filename)
    if args.mode == "SERIAL":
        for blob in find_blobs_with_suffix(gcs_client, args.gcs_path, suffix):
            repost_blob = blob.bucket.blob(
                f"{os.path.dirname(blob.name)}/{repost_filename}"
            )
            upload_empty_blob_and_log(gcs_client, repost_blob)
        return

    assert args.mode == "PARALLEL"
    # These are all I/O bound tasks so use Thread Pool concurrency for speed.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_gsurl = {}
        for blob in find_blobs_with_suffix(gcs_client, args.gcs_path, suffix):
            repost_blob = blob.bucket.blob(
                f"{os.path.dirname(blob.name)}/{repost_filename}"
            )
            future_to_gsurl[executor.submit(
                upload_empty_blob_and_log,
                gcs_client, repost_blob
            )] = f"gs://{blob.bucket.name}/{blob.name}"
        exceptions: Dict[str, Exception] = dict()
        for future in concurrent.futures.as_completed(future_to_gsurl):
            gsurl = future_to_gsurl[future]
            try:
                future.result()
            except Exception as err:  # pylint: disable=broad-except
                logging.error("Error processing %s: %s", gsurl, err)
                exceptions[gsurl] = err
        if exceptions:
            raise RuntimeError("The following errors were encountered:\n" +
                               pprint.pformat(exceptions))
        return


def parse_args() -> argparse.Namespace:
    """argument parser for backfill CLI"""
    parser = argparse.ArgumentParser(
        description="utility to repost GCS blobs for every success blob at a "
                    "GCS prefix")

    parser.add_argument(
        "--gcs-path",
        "-p",
        help="GCS path (e.g. gs://bucket/prefix/to/search/)to search for "
        "existing _SUCCESS files",
        required=True,
    )

    parser.add_argument(
        "--mode",
        "-m",
        help="How to perform the backfill: SERIAL upload the repost files in"
        " order. PARALLEL upload the repost files in parallel (faster)",
        required=False,
        type=str.upper,
        choices=["SERIAL", "PARALLEL"],
        default="PARALLEL",
    )

    parser.add_argument(
        "--success-filename",
        "-f",
        help="Override the default success filename '_SUCCESS'",
        required=False,
        default="_SUCCESS",
    )

    parser.add_argument(
        "--repost-filename",
        "-rf",
        help="This will default to the success-filename",
        required=False,
        default=None,
    )

    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
