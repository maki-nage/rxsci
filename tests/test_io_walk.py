from tempfile import TemporaryDirectory
import os
import shutil

import rx
import rx.operators as ops
from rx.core import Observer
from rxsci.io import walk


def create_file_tree(workdir, directories=[], files=[]):
    for directory in directories:
        os.makedirs(os.path.join(workdir, directory))

    for f in files:
        open(os.path.join(workdir, f), 'a').close()


def test_walk_files():
    expected_files = ['foo', 'bar', 'biz']
    actual_files = []

    with TemporaryDirectory() as workdir:
        def on_next(i):
            nonlocal actual_files
            actual_files.append(i)
        create_file_tree(workdir, files=expected_files)

        walk(workdir).subscribe(
            on_next=on_next,
        )

    actual_files = sorted(actual_files)
    expected_files = [os.path.join(workdir, i) for i in expected_files]
    expected_files = sorted(expected_files)
    assert actual_files == expected_files


def test_walk_dirs():
    expected_files = [
        os.path.join('dfoo', 'foo'),
        os.path.join('dbar', 'bar'),
        os.path.join('dbiz', 'biz'),
    ]
    expected_dirs = ['dfoo', 'dbar', 'dbiz']
    actual_files = []

    with TemporaryDirectory() as workdir:
        def on_next(i):
            print(i)
            nonlocal actual_files
            actual_files.append(i)

        create_file_tree(
            workdir,
            directories=expected_dirs,
            files=expected_files)

        walk(workdir).subscribe(
            on_next=on_next,
        )

    actual_files = sorted(actual_files)
    expected_files = [os.path.join(workdir, i) for i in expected_files]
    expected_files = sorted(expected_files)
    assert actual_files == expected_files
