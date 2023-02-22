from exstruct.parsers import FTPParser
from exstruct.parsers import WebParser
import os
import pytest


def test_sync_sftp():
    ftp_parser = FTPParser(
        source="10.0.45.35:22",
        response_type="xml",  # meh
        user="user",
        password="QaPl91*/5@",
        sync=True,
        timeout=20,
    )
    ftp_parser.parse(
        ".*.txt", start_folder="tests", download_folder="test_results/download"
    )
    folder_content = os.listdir("./test_results/download/")
    assert folder_content == ["test1.txt"]


def test_sync_ftp():
    ftp_parser = FTPParser(
        source="ftp.zakupki.gov.ru",
        response_type="xml",
        user="free",
        password="free",
        sync=True,
        sftp=False,
    )
    ftp_parser.parse(
        ".*.txt", start_folder="", download_folder="test_results/sync_ftp_download"
    )
    folder_content = os.listdir("./test_results/sync_ftp_download/")
    assert folder_content == ["ftp.zakupki.gov.ru"]


def test_async_ftp():
    ftp_parser = FTPParser(
        source="ftp.zakupki.gov.ru",
        response_type="xml",
        user="free",
        password="free",
        sync=False,
        timeout=20,
        sftp=False,
    )
    ftp_parser.parse(
        ".*.txt", start_folder="", download_folder="test_results/async_ftp_download"
    )
    folder_content = os.listdir("./test_results/async_ftp_download/")
    assert folder_content == ["ftp.zakupki.gov.ru"]
