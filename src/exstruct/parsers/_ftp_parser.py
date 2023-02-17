import asyncio
import ftplib
import os
import re
import socket
import threading
from pathlib import Path

import aioftp
import async_timeout
import paramiko

from ..util import _util
from ._base_parser import BaseParser

logger = _util.getLogger("exstruct.parser.ftp_parser")


class FTPParser(BaseParser):
    """Parser for data-sources that provide data via File Transfer Protocol (FTP)

    `Modified FTPSearcher`
    `https://github.com/Sunlight-Rim/FTPSearcher`
    """

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        super().__init__(source, response_type, **kwargs)
        self.sync = kwargs.pop("sync", False)

    def parse(self, search_mask: str = None, **kwargs):
        """Parse FTP-server recursively

        Args:
            search_mask (str, optional): Mask to find specific files. If skipped, matches all files.

        kwargs:
            start_folder (str, optional): Folder to parse. Defaults to root folder ('/')
            download_folder (str, optional): Folder where save content of start_folder. Defaults to 'download/'
            max_lvl (int, optional): Maximum depth level. If skipped, start_folder is parsed completely
            timeout (int, optional): Timeout for establishing connection. Defaults to 10
        """
        self.thread_list = []
        self.port = None
        self.tasks_list = None
        self.start_folder = kwargs.pop("start_folder", "/")
        self.download_folder = kwargs.pop("download_folder", "download")
        self.max_lvl = kwargs.pop("max_lvl", 0)
        self.timeout = kwargs.pop("timeout", 10)
        self.sftp = kwargs.pop("sftp", True)

        if search_mask:
            self.search_mask = search_mask
        else:
            self.search_mask = ".*"

        host, port = self.parse_host_address()
        if self.sync:
            if self.sftp:
                self.sync_sftp_getting(host, *port)
            else:
                self.sync_getting(host, *port)
        else:
            self.tasks_list = []
            self.tasks_list.append(self.async_getting(host, *port, "MLSD", 0))
            ioloop = asyncio.get_event_loop()
            try:
                ioloop.run_until_complete(asyncio.gather(*self.tasks_list))
            except KeyboardInterrupt:
                logger.info("FTP Parser was interrupted via keyboard")
            except NameError:
                pass

    def sync_sftp_getting(self, host: str, port: int):
        host_port = f"{host}:{port}"
        pathlist = [host_port]
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy)
            ssh_client.connect(
                hostname=host,
                port=port,
                username=self.user,
                password=self.password,
                timeout=self.timeout,
            )
            sftp = ssh_client.open_sftp()
            try:
                sftp.chdir(self.start_folder)
            except FileNotFoundError:
                err_msg = f"Path {self.start_folder} doesn't exist on server"
                logger.error(err_msg)

            self.syncnumber = 0
            self.sftp_cycle(folder="", ftp_client=sftp, pathlist=pathlist)

        except OSError as oerr:
            if oerr == "timed out":
                err_msg = f"{host} does not keep a stable connection."
                logger.error(err_msg)
                self.results(host, -2)
            else:
                logger.error(oerr)
        except ftplib.error_perm as msg:
            if msg.args[0][:3] != "530":
                err_msg = f"Login authentication failed onto {host_port}"
            else:
                err_msg = str(msg.args[0][:3])
            logger.error(err_msg)

    def sync_getting(self, host: str, port: int):
        host_port = f"{host}:{port}"
        pathlist = [host_port]
        try:
            ftp = ftplib.FTP(
                host=host,
                timeout=self.timeout,
            )
            ftp.connect(port=port)
            ftp.login(user=self.user, passwd=self.password)

            try:
                ftp.cwd(self.start_folder)
            except ftplib.error_perm:
                err_msg = f"Folder {self.start_folder} not found"
                logger.error(err_msg)

            self.syncnumber = 0
            try:
                self.cycle_inner("", ftp, pathlist)
            except ftplib.error_perm as msg:
                if msg.args[0][:3] == 500:
                    warn_msg = f"MLSD is not supported on server. Trying to use synchronous NLST"
                    logger.warning(warn_msg)
                    self.badftp_cycle("", ftp, pathlist)
            logger.info(f"{host} was crawled")
            ftp.quit()
        except OSError as oerr:
            if oerr == "timed out":
                err_msg = f"{host} does not keep a stable connection."
                logger.error(err_msg)
                self.results(host, -2)
            else:
                logger.error(oerr)
        except ftplib.error_perm as msg:
            if msg.args[0][:3] != "530":
                err_msg = f"Login authentication failed onto {host_port}"
            else:
                err_msg = str(msg.args[0][:3])
            logger.error(err_msg)

    def cycle_inner(self, folder: str, ftp_client: ftplib.FTP, pathlist: list):
        ftp_client.cwd(folder)
        pathlist.append(folder)
        try:
            for file_name, file_facts in filter(
                lambda item: item[0] not in [".", ".."], ftp_client.mlsd()
            ):
                _type = file_facts.get("type")
                if _type not in ["dir", "pdir", "cdir"]:
                    self.searching(file_name, ftp_client, pathlist)
                elif _type == "dir":
                    try:
                        self.cycle_inner(file_name, ftp_client, pathlist)
                    except ftplib.error_perm:
                        err_msg = f"Cannot open the folder {file_name}"
                        logger.error(err_msg)
                    finally:
                        self._exit_folder(ftp_client, pathlist)
        except UnicodeDecodeError:
            err_msg = "This server has cyrillic symbols in the files"
            logger.error(err_msg)
            self.badftp_cycle("", ftp_client, pathlist)

    def searching(self, name: str, ftp_client: ftplib.FTP, pathlist: list):
        full_path = Path(*pathlist, name)
        self.syncnumber += 1
        if re.match(self.search_mask, name):
            self.sync_download(name, ftp_client, full_path)

    def sync_download(self, name: str, ftp_client: ftplib.FTP, full_path: str) -> None:
        try:
            logger.info(f"{full_path} downloading...")
            download_folder = Path(self.download_folder, name)
            os.makedirs(self.download_folder, exist_ok=True)
            fsea = open(download_folder, "wb")
            ftp_client.retrbinary(f"RETR {name}", fsea.write, 8 * 1024)
            fsea.close()
            logger.info("Ok.")
        except KeyboardInterrupt:
            logger.info("You have interrupted file downloading.")
            pass

    def sftp_download(self, name: str, ftp_client: paramiko.SFTPClient, full_path: str):
        try:
            logger.info(f"{full_path} downloading...")
            download_folder = Path(self.download_folder, name)
            os.makedirs(self.download_folder, exist_ok=True)
            ftp_client.get(remotepath=str(full_path), localpath=str(download_folder))
            logger.info("Ok.")
        except KeyboardInterrupt:
            logger.info("You have interrupted file downloading.")

    def badftp_cycle(self, file: str, ftp_client: ftplib.FTP, pathlist: list):
        ftp_client.cwd(file)
        pathlist.append(file)
        file_children = ftp_client.nlst()
        if file_children:
            for file_child in file_children:
                try:
                    self.badftp_cycle(file_child, ftp_client, pathlist)
                    self._exit_folder(ftp_client, pathlist)
                except ftplib.error_perm:
                    self.searching(file_child, ftp_client, pathlist)
                    continue
        else:
            self._exit_folder(ftp_client, pathlist)

    def sftp_cycle(self, folder: str, ftp_client: paramiko.SFTPClient, pathlist: list):
        ftp_client.chdir(folder)
        pathlist.append(folder)
        for file in ftp_client.listdir_iter():
            _type = file.longname[0]
            if _type == "d":
                self.sftp_cycle(
                    folder=file.filename, ftp_client=ftp_client, pathlist=pathlist
                )
                ftp_client.chdir("..")
                pathlist.pop() if len(pathlist) > 1 else None
            elif _type == "-":
                full_path = Path(*pathlist, file.filename)
                self.syncnumber += 1
                if re.match(self.search_mask, file.filename):
                    self.sftp_download(
                        name=file.filename, ftp_client=ftp_client, full_path=full_path
                    )

    async def async_getting(self, host: str, port: int, command: str, asyncnumber: int):
        try:
            client = aioftp.Client()
            async with async_timeout.timeout(self.timeout):
                await client.connect(host, port)
                await client.login(self.user, self.password)
                try:
                    await client.change_directory(self.start_folder)
                except aioftp.StatusCodeError:
                    err_msg = f"Folder {self.start_folder} not found"
                    logger.error(err_msg)
            logger.info(f"{host} started with asynchronous method: {command}")
            await self._async_getting(host, port, command, asyncnumber, client)
        except aioftp.StatusCodeError as exerr:
            self._process_status_code_error(host, port, exerr)
        except ConnectionResetError:
            logger.error(f"{host}:{port} sent a reset package")
        except socket.gaierror:
            logger.error(f"{host}:{port} not responding.")
        except asyncio.TimeoutError:
            logger.error(f"{host}:{port} not responding")
        except OSError as oerr:
            if str(oerr) == "timed out":
                err_msg = f"{host}:{port} not responding"
            elif "[Errno 111] Connect call failed" in str(oerr):
                err_msg = f"{host}:{port} not responding (error 111)"
            elif "[Errno 113] Connect call failed" in str(oerr):
                err_msg = f"{host}:{port} not responding (error 113)"
            elif "[Errno 101] Connect call failed" in str(oerr):
                err_msg = f"{host}:{port} not responding (error 101)"
            logger.error(err_msg)

    async def _async_getting(self, host, port, command, asyncnumber, client):
        try:
            async for path, info in client.list(recursive=True, raw_command=command):
                if self.max_lvl != 0:
                    if str(path).count("/") - 1 >= self.max_lvl:
                        break
                if info["type"] == "file":  # it's better than client.is_file(path)
                    if re.match(self.search_mask, Path(path).name):
                        asyncnumber += 1
                        await self.async_download(host, port, path, asyncnumber)
            print(host + " was crawled.")
        except aioftp.StatusCodeError as inerr:
            if str(inerr.received_codes) == "('500',)":
                if str(inerr.info) == "[' Unknown command.']":
                    logger.warning(
                        f"MLSD is not supported on server {self.source}. Trying to use asynchronous LIST"
                    )
                    await self.async_getting(host, port, "LIST", 0)
                elif "not underst" in str(inerr.info):
                    logger.warning(
                        f"Asynchronous methods is not available on server {self.source}. Trying to use synchronous MLSD"
                    )
                    self.sync_mlsd(host, port)
                else:
                    logger.error(f"{inerr} on {self.source}")
            elif str(inerr.received_codes) == "('550',)":
                logger.error(
                    f"Error 550 (Can't check for file existence) with server {self.source}. Trying to use synchronous MLSD."
                )
                self.sync_mlsd(host, port)
            elif "Waiting for ('1xx',) but got 501" in str(inerr):
                logger.error(
                    f"Error 501 (Not a directory) with server {self.source} Trying to use synchronous MLSD"
                )
                self.sync_mlsd(host, port)
            else:
                logger.error(f"{inerr} on {host}")

    async def async_download(self, host, port, path, asyncnumber):
        full_path = Path(self.source, self.start_folder, path)
        try:
            client2 = aioftp.Client()
            await client2.connect(host, port)
            await client2.login(self.user, self.password)
            logger.info(f"{full_path} downloading...")
            os.makedirs(full_path, exist_ok=True)
            await client2.download(
                path,
                Path(self.download_folder, host, str(path)),
                write_into=True,
            )
        except aioftp.errors.PathIOError:
            logger.error("Unable to download files. Check out your privileges.")

    def sync_mlsd(self, host, port):
        t = threading.Thread(
            target=self.sync_getting,
            name="Thread " + host + str(port),
            args=(host, port),
        )
        self.thread_list.append(t)
        t.start()

    def _process_status_code_error(self, host, port, err):
        if str(
            err.received_codes
        ) == "('530',)" or "Waiting for ('230', '33x') but got 421 [' Unable to set up secure anonymous FTP']" in str(
            err
        ):
            err_msg = f"Login authentication failed onto {host}:{port}"
        elif "Waiting for ('230', '33x') but got 421 [\" Can't change directory" in str(
            err
        ):
            err_msg = f"Can't change directory on {host}"
        elif (
            str(err)
            == "Waiting for ('230', '33x') but got 550 [\" Can't set guest privileges.\"]"
        ):
            err_msg = f"Can't set guest privileges on {host}"
        elif (
            str(err)
            == "Waiting for ('220',) but got 501 [' Proxy unable to contact ftp server']"
        ):
            err_msg = f"Proxy unable to contact {host}"
        elif (
            str(err)
            == "Waiting for ('220',) but got 550 [' No connections allowed from your IP']"
            or str(err)
            == "Waiting for ('230', '33x') but got 421 [' Temporarily banned for too many failed login attempts']"
        ):
            err_msg = f"Your IP was banned on {host}"
        else:
            err_msg = f"{err} on {host}"
        logger.error(err_msg)

    def parse_host_address(self, host_address: str = None):
        if host_address is None:
            host_address = self.source
        host, *port = host_address.split(":")
        port = tuple(map(int, port)) if port else (21,)
        return host, port

    def _exit_folder(self, ftp: ftplib.FTP, pathlist: list[str]):
        ftp.sendcmd("cdup")
        pathlist.pop() if len(pathlist) > 1 else None
