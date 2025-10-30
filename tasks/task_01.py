# -*- coding: utf-8 -*-

"""
HomeWork Task 1
"""

import asyncio
import argparse
import logging
import time

from aiopath import AsyncPath
from aioshutil import copyfile
from collections import defaultdict
from typing import Optional, Union


_file_name_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


async def get_absolute_path(path: Union[AsyncPath, str], current_dir: Optional[Union[AsyncPath, str]] = None) -> AsyncPath:
    """
    Return the absolute path for the given path and the current directory.

    :param path: specified path (str, Path, mandatory)
    :param current_dir: current directory (str, Path, optional)
    :return: absolute path (Path)
    """
    if not path:
        # The path can not be None or an empty string
        raise ValueError('The path can not be empty')

    if AsyncPath(path).is_absolute():
        # If the specified path is an absolute - return it
        return AsyncPath(path)

    if current_dir is not None and isinstance(current_dir, str):
        # If the current directory is not specified, use the current working directory
        current_dir = AsyncPath(current_dir)

    # Construct an absolute path and return
    return (current_dir if current_dir is not None else await AsyncPath.cwd()) / path


async def file_path_build(file: AsyncPath, dest: AsyncPath) -> AsyncPath:
    """
    Build a new file name based on the destination folder and file extension

    :param file: Absolute path of the existing file
    :param dest: Destination folder
    :return: New file absolute path
    """

    # Build a new file name based on the destination folder and file extension
    file_extension: str = file.suffix[1:] if file.suffix.startswith(".") else file.suffix
    file_name: str = file.stem
    new_file_name: AsyncPath = dest / (file_extension or "without_extension") / f"{file_name}.{file_extension}"

    # Verify if a file with the same name already exists
    rename_tries: int = 0
    while await new_file_name.exists():
        rename_tries += 1
        # Build a new file name with index of copy
        new_file_name = new_file_name.with_stem(f"{file_name} ({rename_tries})")

    return new_file_name


async def read_folder(source_folder: AsyncPath) -> list[AsyncPath]:
    logging.info(f"Process folder: {source_folder.as_posix()}")

    files: list[AsyncPath] = []

    try:
        async for child in source_folder.iterdir():
            if await child.is_dir():
                files.extend(await read_folder(child))
            elif await child.is_file():
                files.append(child)
            else:
                # symbolic links/devices, etc. — ignore them
                logging.warn(f"Skip non-regular: {child.as_posix()}")
    except OSError as e:
        logging.info(f"Failed to process folder \"{source_folder.as_posix()}\": {str(e)}")
    finally:
        return files


async def copy_file(file: AsyncPath, folder: AsyncPath):
    lock = _file_name_locks[file.name.lower()]
    async with lock:
        # Build a new file name based on the destination folder and file extension
        new_file = await file_path_build(file, folder)
        try:
            # Create a new folder if it does not already exist
            await new_file.parent.mkdir(parents=True, exist_ok=True)
            # Copy the file
            await copyfile(file, new_file)
            logging.info(f"File: {file.as_posix()} copied to {new_file.as_posix()}")
        except OSError as e:
            logging.error(f'Failed to copy file "{file.as_posix()}" to "{new_file.as_posix()}": {str(e)}')


async def folder_copy(source: str, dest: str) -> None:
    """
    Asynchronous sorting of files by extension and copying them from the source folder to the destination folder.

    :param source: Source folder
    :param dest: Destination folder
    """

    # Make paths asynchronous
    source_folder: AsyncPath = await get_absolute_path(source)
    dest_folder: AsyncPath = await get_absolute_path(dest)

    files: list[AsyncPath] = await read_folder(source_folder)
    await asyncio.gather(*[copy_file(file, dest_folder) for file in files])


def cli() -> None:
    try:
        root_logger = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)

        parser = argparse.ArgumentParser(
            description="Copy and sort the files from source folder to destination folder",
            epilog="Good bye!")
        parser.add_argument("-s", "--source", type=str, required=True, help="Source folder")
        parser.add_argument("-o", "--output", type=str, default="dist", help="output folder (default \"dist\")")

        args = parser.parse_args()

        start_time: float = time.perf_counter()
        asyncio.run(folder_copy(args.source, args.output))
        logging.info(f"File copying completed (execution time: {(time.perf_counter() - start_time):.06f} seconds)")
    except Exception as e:
        logging.error(e)

    exit(0)
