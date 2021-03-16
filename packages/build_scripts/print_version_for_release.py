import ast
import os
import re
import sys

import packaging.version


DEV_RELEASE = os.environ.get("DEV_RELEASE", None) == "1"
GALAXY_RELEASE = os.environ.get("GALAXY_RELEASE", None) == "1"

GALAXY_VERSION_FILE_PATH = os.path.join(os.getcwd(), os.pardir, os.pardir, 'lib', 'galaxy', 'version.py')

PROJECT_DIRECTORY = os.getcwd()
PROJECT_DIRECTORY_NAME = os.path.basename(os.path.abspath(PROJECT_DIRECTORY))
PROJECT_MODULE_FILENAME = "project_galaxy_%s.py" % PROJECT_DIRECTORY_NAME

source_dir = sys.argv[1]
PROJECT_MODULE_PATH = os.path.join(PROJECT_DIRECTORY, source_dir, PROJECT_MODULE_FILENAME)


def read_from_galaxy():
    _version_major_re = re.compile(r'^VERSION_MAJOR\s+=\s+(.*)', re.MULTILINE)
    _version_minor_re = re.compile(r'^VERSION_MINOR\s+=\s+(.*)', re.MULTILINE)
    with open(GALAXY_VERSION_FILE_PATH, 'rb') as f:
        contents = f.read().decode('utf-8')
        version_major = str(ast.literal_eval(_version_major_re.search(contents).group(1)))
        version_minor = ast.literal_eval(_version_minor_re.search(contents).group(1))
        if version_minor is None:
            version = f'{version_major}.0'
        elif version_minor == 'dev':
            version = f'{version_major}.0.dev0'
        elif version_minor:
            try:
                version_minor = int(version_minor[0])
            except (TypeError, ValueError):
                version_minor = f'0.{version_minor}'
            version = f'{version_major}.{version_minor}'
        else:
            version = version_major
    return version


def read_from_package():
    _version_re = re.compile(r'__version__\s+=\s+(.*)')
    with open(PROJECT_MODULE_PATH, 'rb') as f:
        version = str(ast.literal_eval(_version_re.search(
            f.read().decode('utf-8')).group(1)))
    return version


def main():
    version = read_from_galaxy() if GALAXY_RELEASE else read_from_package()
    if not DEV_RELEASE:
        # Strip .devN
        version_tuple = packaging.version.parse(version).release
        print(".".join(map(str, version_tuple)))
    else:
        print(packaging.version.parse(version))


if __name__ == '__main__':
    main()
