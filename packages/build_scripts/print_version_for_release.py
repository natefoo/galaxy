import ast
import os
import re
import sys
from distutils.version import LooseVersion

#import packaging.version


DEV_RELEASE = os.environ.get("DEV_RELEASE", None) == "1"
# FIXME: var for inter-point release?
#PROJECT_DIRECTORY = os.getcwd()
#PROJECT_DIRECTORY_NAME = os.path.basename(os.path.abspath(PROJECT_DIRECTORY))
#PROJECT_MODULE_FILENAME = "project_galaxy_%s.py" % PROJECT_DIRECTORY_NAME

#source_dir = sys.argv[1]
#PROJECT_MODULE_PATH = os.path.join(PROJECT_DIRECTORY, source_dir, PROJECT_MODULE_FILENAME)

GALAXY_VERSION_FILE_PATH = os.path.join(os.getcwd(), os.pardir, os.pardir, 'lib', 'galaxy', 'version.py')

_version_major_re = re.compile(r'^VERSION_MAJOR\s+=\s+(.*)', re.MULTILINE)
_version_minor_re = re.compile(r'^VERSION_MINOR\s+=\s+(.*)', re.MULTILINE)
#with open(PROJECT_MODULE_PATH, 'rb') as f:
with open(GALAXY_VERSION_FILE_PATH, 'rb') as f:
    contents = f.read().decode('utf-8')
    version_major = str(ast.literal_eval(_version_major_re.search(contents).group(1)))
    version_minor = ast.literal_eval(_version_minor_re.search(contents).group(1))
    if version_minor:
        version = f'{version_major}.{version_minor}'
    else:
        version = version_major

if not DEV_RELEASE:
    # Strip .devN
    #version_tuple = packaging.version.parse(version).release
    version_tuple = LooseVersion(version).version[0:3]
    print(".".join(map(str, version_tuple)))
else:
    #print(packaging.version.parse(version))
    print(version)
