#!/usr/bin/env python
import subprocess
import glob
import os

from typing import Dict, List


def decompress_lzo(file) -> List[Dict]:
    """Decompress lzo file, see https://www.lzop.org/lzop_man.php

    :param file: file to decompress
    :return: the output from the decompressed file
    """

    # with run() no need to take care of communicate since
    # it is built in, nice!
    p = subprocess.run(
        f'lzop -cd {file}',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if p.returncode != 0:
        raise ValueError(f'{p.returncode}:{p.stderr}')
    else:

        # remove any lzo file on the executor host
        for fl in glob.glob("*.lzo"):
            os.remove(fl)

        # decode output from byte to str
        output = p.stdout.decode('utf-8').splitlines()

        return output
