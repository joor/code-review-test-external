"""
Tests for the FTP utility methods
"""
import ftplib
import io
import unittest
import unittest.mock
import unittest.mock

import ftp.utils
from ftp.tests.ftpstub import FTPStubServer


# pylint: skip-file


class FTPFileLoaderUtilsTest(unittest.TestCase):
    """
    FTPFileLoader tests that use the stub FTP client.
    FTP stub runs and FTP server when setting up a test.

    All file operations are performed in the memory.
    When uploading the data, mocking of the file operations is used.
    When downloading the data, the download handlers are used
    for memory only operations.
    """

    def setUp(self):
        self.sample_file_name = 'sample.txt'
        self.sample_file_path = 'test/data'
        self.sample_file_content = 'Sample file'

        self.server = FTPStubServer(port=0, hostname='127.0.0.1')
        self.server.run()

        self.host = self.server.server.server_address[0]
        self.port = self.server.server.server_address[1]
        self.ftp = ftplib.FTP()
        ftp.utils.connect_and_login(self.ftp, self.host, self.port)

    def tearDown(self):
        self.ftp.close()
        self.server.stop()

    @unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=b'data'))
    @unittest.mock.patch("os.path.isfile", unittest.mock.MagicMock())
    @unittest.mock.patch("os.path.exists", unittest.mock.MagicMock())
    def test_file_is_uploaded_to_ftp(self):
        """
        To test that the data is uploaded to the FTP server a sample file
        is uploaded to specified ftp path and it
        content and name is not changed.
        """
        ftp_path = 'test/data/'

        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path)

        self.assertEqual(self.ftp.pwd(), ftp_path)
        self.assertTrue(self.server.files(self.sample_file_name))

    @unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=b'data'))
    @unittest.mock.patch("os.path.isfile", unittest.mock.MagicMock())
    def test_file_is_uploaded_not_to_ftp_if_it_doesnot_exist(self):
        """
        By giving a non-existing path (not-mocking it) to a file it is checked that
        the exception is thrown and file is not created
        """
        ftp_path = 'test/data/'
        with self.assertRaises(Exception):
            ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path)

        self.assertFalse(self.server.files(self.sample_file_name))

    @unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=b'data'))
    @unittest.mock.patch("os.path.isfile", unittest.mock.MagicMock())
    @unittest.mock.patch("os.path.exists", unittest.mock.MagicMock())
    def test_file_upload_as_content_manager(self):
        """
        To test that the FTP client can work as a content manager
        it uploads a file this way with no errors.
        """
        ftp_path = 'test/data/'

        with ftp.utils.FTPLogErrors(ftplib.FTP()) as ftp_client:
            ftp.utils.connect_and_login(ftp_client, host=self.host, port=self.port)
            ftp.utils.upload_file(ftp_client, self.sample_file_path, self.sample_file_name, ftp_path)

    @unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=b'data'))
    @unittest.mock.patch("os.path.isfile", unittest.mock.MagicMock())
    @unittest.mock.patch("os.path.exists", unittest.mock.MagicMock())
    def test_unique_names_are_generated_for_similar_uploaded_files(self):
        """
        Sample file with the same name loaded multiple times
        ends up having unique name in the target directory.
        """
        ftp_path = 'test/data/'

        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path)
        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path)
        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path)

        self.assertEqual(len(self.ftp.nlst()), 3)

    @unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=b'data'))
    @unittest.mock.patch("os.path.isfile", unittest.mock.MagicMock())
    @unittest.mock.patch("os.path.exists", unittest.mock.MagicMock())
    def test_similar_files_are_rewritten(self):
        """
        Sample file with the same name loaded multiple times
        ends up being rewritten if the options is specified
        """
        ftp_path = 'test/data/'

        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path, rename_existing=True)
        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path, rename_existing=True)
        ftp.utils.upload_file(self.ftp, self.sample_file_path, self.sample_file_name, ftp_path, rename_existing=True)

        self.assertEqual(len(self.ftp.nlst()), 1)

    def test_subdirectories_are_created_moved_to_basepath(self):
        """
        Sub-directories provided are created and the FTP
        client is in the created directory.
        If directory is not created an exception is raised,
        if the code doesn't raise an exception, the directory is created.
        """
        base_path = '/'
        ftp_path = 'test/data/'
        ftp.utils.makedirs(self.ftp, ftp_path, base_directory=base_path)
        self.assertEqual(self.ftp.pwd(), base_path)

    def test_file_is_downloaded_from_ftp(self):
        """
        Uploaded file can be downloaded without content
        and name changes.
        """
        self.server.add_file(self.sample_file_name, self.sample_file_content)

        dst = io.BytesIO()

        def handler(data):
            return dst.write(data)

        ftp.utils.download_file(self.ftp, '', self.sample_file_name, handler)

        dst_content = dst.getvalue().decode('utf-8')
        self.assertEqual(dst_content, self.sample_file_content)

    def test_path_is_downloaded_from_ftp(self):
        """
        Files can be downloaded from specified FTP
        path without content changes,
        which means the download function works as expected.
        """
        ftp_path = '/data'

        self.server.add_file(self.sample_file_name, self.sample_file_content)
        self.server.add_file(self.sample_file_name + '1', self.sample_file_content)

        accum = []

        def handler(data):
            dst = io.BytesIO()
            dst.write(data)
            accum.append(dst)

        ftp.utils.download_directory(self.ftp, '/', ftp_path, download_handler=handler)

        self.assertEqual(len(accum), 2)

        for data in accum:
            dst_content = data.getvalue().decode('utf-8')
            self.assertEqual(dst_content, self.sample_file_content)

    def test_path_is_downloaded_from_ftp_with_name_filter(self):
        """
        Adds a few files to the FTP server and download the directory,
        applying the name filter.
        The number of downloaded files is less then the number
        of initial file because of the filter.
        """
        ftp_path = '/data'

        self.server.add_file(self.sample_file_name, self.sample_file_content)
        self.server.add_file(self.sample_file_name + '1', self.sample_file_content)
        self.server.add_file(self.sample_file_name + '2', self.sample_file_content)

        def name_filter(name):
            # Do not include third file with *-2.ext name
            return '2' not in name

        accum = []

        def handler(data):
            dst = io.BytesIO()
            dst.write(data)
            accum.append(dst)

        ftp.utils.download_directory(self.ftp, "/", ftp_path, name_filter=name_filter, download_handler=handler)

        self.assertEqual(len(accum), 2)

        for data in accum:
            dst_content = data.getvalue().decode('utf-8')
            self.assertEqual(dst_content, self.sample_file_content)

    def test_cannot_connect(self):
        """
        Raises an exception when cannot connect to a server.
        """
        with self.assertRaises(BaseException):
            ftp_client = ftplib.FTP()
            ftp.utils.connect_and_login(ftp_client, host='invalidhost', port=self.port)
