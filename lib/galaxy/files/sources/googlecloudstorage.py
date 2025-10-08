try:
    from fs_gcsfs import GCSFS
    from google.cloud.storage import Client
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials
except ImportError:
    GCSFS = None

import os
from typing import (
    cast,
    List,
    Optional,
    Tuple,
)

from . import (
    AnyRemoteEntry,
    FilesSourceOptions,
    FilesSourceProperties,
    RemoteDirectory,
    RemoteFile,
)
from ._pyfilesystem2 import PyFilesystem2FilesSource


class GoogleCloudStorageFilesSourceProperties(FilesSourceProperties, total=False):
    bucket_name: str
    root_path: str
    project: str
    anonymous: bool
    service_account_json: str
    token: str
    token_uri: str
    client_id: str
    client_secret: str
    refresh_token: str


class GoogleCloudStorageFilesSource(PyFilesystem2FilesSource):
    plugin_type = "googlecloudstorage"
    required_module = GCSFS
    required_package = "fs-gcsfs"

    def _open_fs(self, user_context=None, opts: Optional[FilesSourceOptions] = None):
        props = self._serialization_props(user_context)
        extra_props: GoogleCloudStorageFilesSourceProperties = cast(
            GoogleCloudStorageFilesSourceProperties, opts.extra_props or {} if opts else {}
        )
        bucket_name = props.pop("bucket_name", None)
        root_path = props.pop("root_path", None)
        project = props.pop("project", None)
        service_account_json = props.pop("service_account_json", None)
        args = {}
        if props.get("anonymous"):
            args["client"] = Client.create_anonymous_client()
        elif service_account_json:
            credentials = service_account.Credentials.from_service_account_file(service_account_json)
            args["client"] = Client(project=project, credentials=credentials)
        elif props.get("token"):
            args["client"] = Client(project=project, credentials=Credentials(**props))
        handle = GCSFS(bucket_name, root_path=root_path, retry=0, **{**args, **extra_props})
        return handle

    def _list(
        self,
        path="/",
        recursive=False,
        user_context=None,
        opts: Optional[FilesSourceOptions] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        query: Optional[str] = None,
        sort_by: Optional[str] = None,
    ) -> Tuple[List[AnyRemoteEntry], int]:
        """
        Override base class _list to work around fs_gcsfs limitation with virtual directories.

        GCS doesn't require directory marker objects, but fs_gcsfs's getinfo() requires them.
        This implementation uses the GCS API directly to list blobs, bypassing the problematic
        getinfo() validation that fails for virtual directories.
        """
        if recursive:
            # For recursive listing, fall back to the base implementation
            return super()._list(path, recursive, user_context, opts, limit, offset, query, sort_by)

        # Open filesystem to get access to the bucket
        with self._open_fs(user_context=user_context, opts=opts) as fs_handle:
            # Access the bucket from the GCSFS object
            bucket = fs_handle.bucket

            # Convert path to GCS prefix format
            # Remove leading/trailing slashes and add trailing slash for directory prefix
            normalized_path = path.strip("/")
            if normalized_path:
                prefix = normalized_path + "/"
            else:
                prefix = ""

            # List blobs with delimiter to get immediate children only (non-recursive)
            delimiter = "/"

            # Collect directories (prefixes) and files (blobs)
            entries: List[AnyRemoteEntry] = []

            # First iterator: Get directories from prefixes
            page_iterator_dirs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
            for page in page_iterator_dirs.pages:
                for dir_prefix in page.prefixes:
                    # Remove the parent prefix and trailing slash to get just the dir name
                    dir_name = dir_prefix[len(prefix) :].rstrip("/")
                    if dir_name:
                        full_path = os.path.join("/", normalized_path, dir_name) if normalized_path else f"/{dir_name}"
                        uri = self.uri_from_path(full_path)
                        entries.append(RemoteDirectory(name=dir_name, uri=uri, path=full_path))

            # Second iterator: Get files from blobs
            page_iterator_files = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
            for blob in page_iterator_files:
                # Skip directory marker objects (empty blobs ending with /)
                if blob.name.endswith("/"):
                    continue

                # Get just the filename (remove prefix)
                file_name = blob.name[len(prefix) :]
                if file_name:
                    full_path = os.path.join("/", normalized_path, file_name) if normalized_path else f"/{file_name}"
                    uri = self.uri_from_path(full_path)

                    # Convert blob metadata to RemoteFile
                    ctime = None
                    if blob.time_created:
                        ctime = blob.time_created.isoformat()

                    entries.append(
                        RemoteFile(name=file_name, size=blob.size or 0, ctime=ctime, uri=uri, path=full_path)
                    )

            # Apply query filter if provided
            if query:
                query_lower = query.lower()
                entries = [e for e in entries if query_lower in e.name.lower()]

            # Get total count before pagination
            total_count = len(entries)

            # Apply pagination
            if offset is not None or limit is not None:
                start = offset or 0
                end = start + limit if limit is not None else None
                entries = entries[start:end]

            return entries, total_count

    def _realize_to(
        self,
        source_path: str,
        native_path: str,
        user_context=None,
        opts: Optional[FilesSourceOptions] = None,
    ):
        """
        Override to download files directly from GCS, bypassing fs_gcsfs's directory marker checks.
        """
        with self._open_fs(user_context=user_context, opts=opts) as fs_handle:
            bucket = fs_handle.bucket

            # Convert path to GCS blob key
            normalized_path = source_path.strip("/")

            # Get the blob
            blob = bucket.get_blob(normalized_path)
            if not blob:
                raise Exception(f"File not found: {source_path}")

            # Download directly to file
            with open(native_path, "wb") as write_file:
                blob.download_to_file(write_file)

    def _write_from(
        self,
        target_path: str,
        native_path: str,
        user_context=None,
        opts: Optional[FilesSourceOptions] = None,
    ):
        """
        Override to upload files directly to GCS, bypassing fs_gcsfs's directory marker checks.
        """
        with self._open_fs(user_context=user_context, opts=opts) as fs_handle:
            bucket = fs_handle.bucket

            # Convert path to GCS blob key
            normalized_path = target_path.strip("/")

            # Create blob and upload
            blob = bucket.blob(normalized_path)
            with open(native_path, "rb") as read_file:
                blob.upload_from_file(read_file)


__all__ = ("GoogleCloudStorageFilesSource",)
