"""
A set of utility methods to encapsulate common
logic of work with the FTP client.

Since FTP servers might fail to process long-running
connection or many connections, the FTP utility methods
use retries on exceptions to download/upload files.

By default the wrapper works in the passive FTP mode.

There's a context manager to automatically close the connection
and log errors:

    with FTPLogErrors(ftplib.FTP()) as ftp_client:
        connect_and_login(ftp_client, host=self.host, port=self.port)
        ...
"""
import contextlib
import ftplib
import logging
import os
import typing

import tenacity

logger = logging.getLogger(__name__)

retry = tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(5),
                       retry=tenacity.retry_if_exception_type(ftplib.all_errors))


# pylint: disable=too-many-arguments
# pylint: disable=bad-whitespace


class FTPLogErrors(contextlib.closing):
    """
    This context manager is used to handle
    closing of an FTP connection with errors logged:

        with FTPLogErrors(ftplib.FTP()) as ftp_client:
            pass

    """

    def __exit__(self, *exc_info):
        _, exc_val, _ = exc_info
        if exc_val:
            logger.error(exc_val, exc_info=True)
        self.thing.close()


@retry
def connect_and_login(ftp_client: ftplib.FTP, host: str = '', port: int = 0, user: str = '', passwd: str = '',
                      timeout: str = -999) -> None:
    """Connects to the FTP server and logins with retries"""
    ftp_client.connect(host=host, port=port, timeout=timeout)
    ftp_client.login(user, passwd)

    # Passive by default
    ftp_client.set_pasv(1)


@retry
def download_file(ftp_client: ftplib.FTP,
                  local_file_path: str,
                  ftp_file_path: str,
                  download_handler: typing.Callable[[bytes], None] = None) -> None:
    """
    Downloads a file from the FTP server to the local path.
    Both source and destination files are specified by its full paths.
    The download handler might be used to specify where and
    how files are written (to the memory, to the database, etc.).
    By default they are written to files in the folder specified.
    Retries on FTP-related exceptions.
    """
    return _download(ftp_client, local_file_path=local_file_path, ftp_file_path=ftp_file_path,
                     download_handler=download_handler)


@retry
def upload_file(ftp_client: ftplib.FTP, local_directory: str, file_name: str, ftp_directory: str,
                rename_existing: bool = False) -> str:
    """
    Uploads a local file to the FTP directory.
    File is specified by its local directory and its name in it.
    If renaming is requested, uploaded files with similar names
    get unique names in form *-number.ext, otherwise file are rewritten.
    Returns file output name and error (if any).
    Retries on FTP-related exceptions.
    """
    file_path = os.path.join(local_directory, file_name)
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        raise OSError('Does not exist {}'.format(file_path))

    makedirs(ftp_client, ftp_directory)
    ftp_client.cwd(ftp_directory)

    out_name = file_name
    if not rename_existing:
        out_name = get_unique_file_name_if_exists(ftp_client, file_name)
    with open(file_path, 'rb') as file:
        ftp_client.storbinary('STOR {}'.format(out_name), file)
    return os.path.join(out_name)


@retry
def download_directory(ftp_client: ftplib.FTP,
                       local_directory: str,
                       ftp_directory: str,
                       name_filter: typing.Callable[[str], bool] = None,
                       download_handler: typing.Callable[[bytes], None] = None) -> None:
    """
    Downloads all files in an FTP directory to a local directory.
    Files are overwritten if they exist.
    The name filter function might be used to exclude certain file names.
    The download handler might be used to specify where and
    how files are written (to memory, to database, etc.).
    By default they are written to files in the folder specified.
    FTP client is moved to source ftp directory.
    Retries on FTP-related exceptions.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    ftp_client.cwd(ftp_directory)

    files = list_files(ftp_client)

    for file in files:
        if name_filter and not name_filter(file):
            continue
        _download(ftp_client, os.path.join(local_directory, file), file, download_handler)


def _download(ftp_client: ftplib.FTP,
              local_file_path: str,
              ftp_file_path: str,
              download_handler: typing.Callable[[bytes], None] = None) -> None:
    """
    Downloads a file from the FTP server to the local path.
    Both source and destination files are specified by its full paths.
    The download handler might be used to specify where and
    how files are written (to memory, to database, etc.).
    By default they are written to files in the folder specified.
    """
    file = None
    if not download_handler:
        file = open(local_file_path, 'wb')
        download_handler = file.write

    ftp_client.retrbinary('RETR {}'.format(ftp_file_path), download_handler)

    if file:
        file.close()


def makedirs(ftp_client: ftplib.FTP, path: str, base_directory: str = '/') -> None:
    """
    Creates sub directories using specified base directory as a root.
    Moves the FTP client to the directory created.
    """
    ftp_client.cwd(base_directory)

    subpaths = path.strip('/').split('/')
    for subpath in subpaths:
        ftp_client.mkd(subpath)
        ftp_client.cwd(subpath)

    ftp_client.cwd(base_directory)


def move_file(ftp_client: ftplib.FTP, ftp_src_directory: str, ftp_dst_directory: str, src_filename: str,
              rename_existing: bool = False) -> str:
    """
    Moves specified file from src to dst folder on the FTP server.
    If renaming is requested, uploaded files with similar names
    get unique names in form *-number.ext, otherwise file are rewritten.
    Returns destination file path.
    """
    newname = src_filename
    if not rename_existing:
        newname = get_unique_file_name_if_exists(ftp_client, src_filename, ftp_dst_directory)

    oldpath = os.path.join(ftp_src_directory, src_filename)
    newpath = os.path.join(ftp_dst_directory, newname)

    ftp_client.rename(oldpath, newpath)
    return newname


def list_files(ftp_client: ftplib.FTP, directory: str = '') -> typing.List[str]:
    """
    A thin wrapper to lists file names in specified directory or in
    the current directory (by default). It uses more clear name for outside code and
    limits number of directories to only one.
    """
    return ftp_client.nlst(directory)


def get_unique_file_name_if_exists(ftp_client: ftplib.FTP, file_name: str, directory: str = '') -> str:
    """
    Returns unique file name in the given directory (or current directory).
    Unique names have the form basename-uniquenumber.ext
    """
    names = list_files(ftp_client, directory)
    names = set(names)
    suffix = 1
    newname = file_name
    while newname in names:
        root, ext = os.path.splitext(file_name)
        newname = '{}-{}{}'.format(root, suffix, ext)
        suffix += 1
    return newname
