# Location of virtualenv used for development.
VENV?=.venv
# Source virtualenv to execute command (flake8, sphinx, twine, etc...)
IN_VENV=if [ -f "$(VENV)/bin/activate" ]; then . "$(VENV)/bin/activate"; fi;
RELEASE_CURR:=16.01
RELEASE_CURR_MINOR_NEXT:=$(shell $(IN_VENV) python scripts/bootstrap_history.py --print-next-minor-version)
RELEASE_NEXT:=16.04
RELEASE_LOCAL_TAG?=local
RELEASE_LOCAL_COMMIT?=$(shell git rev-parse --short HEAD)
RELEASE_LOCAL_VERSION?=$(RELEASE_LOCAL_TAG)$(shell date -u +%Y%m%dT%H%M%SZ).$(RELEASE_LOCAL_COMMIT)
RELEASE_SOURCE_BRANCH?=dev
# TODO: This needs to be updated with create_release_rc
#RELEASE_NEXT_BRANCH:=release_$(RELEASE_NEXT)
RELEASE_NEXT_BRANCH:=dev
RELEASE_UPSTREAM:=upstream
MY_UPSTREAM:=origin
CONFIG_MANAGE=$(IN_VENV) python lib/galaxy/config/config_manage.py
PROJECT_URL?=https://github.com/galaxyproject/galaxy
DOCS_DIR=doc
DOC_SOURCE_DIR=$(DOCS_DIR)/source
SLIDESHOW_DIR=$(DOC_SOURCE_DIR)/slideshow
OPEN_RESOURCE=bash -c 'open $$0 || xdg-open $$0'
SLIDESHOW_TO_PDF?=bash -c 'docker run --rm -v `pwd`:/cwd astefanutti/decktape /cwd/$$0 /cwd/`dirname $$0`/`basename -s .html $$0`.pdf'
YARN := $(shell command -v yarn 2> /dev/null)

all: help
	@echo "This makefile is used for building Galaxy's JS client, documentation, and drive the release process. A sensible all target is not implemented."

docs: ## Generate HTML documentation.
# Run following commands to setup the Python portion of the requirements:
#   $ ./scripts/common_startup.sh
#   $ . .venv/bin/activate
#   $ pip install -r lib/galaxy/dependencies/dev-requirements.txt
	$(IN_VENV) $(MAKE) -C doc clean
	$(IN_VENV) $(MAKE) -C doc html

docs-develop: ## Fast doc generation and more warnings (for development)
	$(IN_VENV) GALAXY_DOCS_SKIP_VIEW_CODE=1 SPHINXOPTS='-j 4' $(MAKE) -C doc html

setup-venv:
	if [ ! -f $(VENV)/bin/activate ]; then bash scripts/common_startup.sh --dev-wheels; fi

list-dependency-updates: setup-venv
	$(IN_VENV) pip list --outdated --format=columns

docs-slides-ready:
	test -f plantuml.jar ||  wget http://jaist.dl.sourceforge.net/project/plantuml/plantuml.jar
	java -jar plantuml.jar -c $(DOC_SOURCE_DIR)/slideshow/architecture/images/plantuml_options.txt -tsvg $(SLIDESHOW_DIR)/architecture/images/ *.plantuml.txt
	$(IN_VENV) python scripts/slideshow/build_slideshow.py 'Galaxy Architecture' $(SLIDESHOW_DIR)/architecture/galaxy_architecture.md

docs-slides-export: docs-slides-ready
	$(SLIDESHOW_TO_PDF) $(SLIDESHOW_DIR)/galaxy_architecture/galaxy_architecture.html

_open-docs:
	$(OPEN_RESOURCE) $(DOCS_DIR)/_build/html/index.html

open-docs: docs _open-docs ## generate Sphinx HTML documentation and open in browser

open-project: ## open project on github
	$(OPEN_RESOURCE) $(PROJECT_URL)

uwsgi-rebuild-validation: ## rebuild uwsgi_config.yml kwalify schema against latest uwsgi master.
	$(CONFIG_MANAGE) build_uwsgi_yaml

tool-shed-config-validate: ## validate tool shed YAML configuration file
	$(CONFIG_MANAGE) validate tool_shed

tool-shed-config-lint: ## lint tool shed YAML configuration file
	$(CONFIG_MANAGE) lint tool_shed

tool-shed-config-convert-dry-run: ## convert old style tool shed ini to yaml (dry run)
	$(CONFIG_MANAGE) convert tool_shed --dry-run

tool-shed-config-convert: ## convert old style tool shed ini to yaml
	$(CONFIG_MANAGE) convert tool_shed

reports-config-validate: ## validate reports YAML configuration file
	$(CONFIG_MANAGE) validate reports

reports-config-convert-dry-run: ## convert old style reports ini to yaml (dry run)
	$(CONFIG_MANAGE) convert reports --dry-run

reports-config-convert: ## convert old style reports ini to yaml
	$(CONFIG_MANAGE) convert reports

reports-config-lint: ## lint reports YAML configuration file
	$(CONFIG_MANAGE) lint reports

config-validate: ## validate galaxy YAML configuration file
	$(CONFIG_MANAGE) validate galaxy

config-convert-dry-run: ## convert old style galaxy ini to yaml (dry run)
	$(CONFIG_MANAGE) convert galaxy --dry-run

config-convert: ## convert old style galaxy ini to yaml
	$(CONFIG_MANAGE) convert galaxy

config-rebuild: ## Rebuild all sample YAML and RST files from config schema
	$(CONFIG_MANAGE) build_sample_yaml galaxy --add-comments
	$(CONFIG_MANAGE) build_rst galaxy > doc/source/admin/galaxy_options.rst
	$(CONFIG_MANAGE) build_sample_yaml reports --add-comments
	$(CONFIG_MANAGE) build_rst reports > doc/source/admin/reports_options.rst
	$(CONFIG_MANAGE) build_sample_yaml tool_shed --add-comments

config-lint: ## lint galaxy YAML configuration file
	$(CONFIG_MANAGE) lint galaxy

release-ensure-upstream: ## Ensure upstream branch for release commands setup
ifeq (shell git remote -v | grep $(RELEASE_UPSTREAM), )
	git remote add $(RELEASE_UPSTREAM) git@github.com:galaxyproject/galaxy.git
else
	@echo "Remote $(RELEASE_UPSTREAM) already exists."
endif

release-ensure-prereqs: ## Ensure venv exists with packaging module and the source is clean
	git diff --quiet || { echo "Error: some files have changes"; exit 1; }
	$(IN_VENV) pip show packaging

release-merge-stable-to-next: release-ensure-upstream ## Merge last release into dev
	git fetch $(RELEASE_UPSTREAM) && git checkout dev && git merge --ff-only $(RELEASE_UPSTREAM)/dev && git merge $(RELEASE_UPSTREAM)/$(RELEASE_PREVIOUS)

release-push-dev: release-ensure-upstream # Push local dev branch upstream
	git push $(RELEASE_UPSTREAM) dev

release-issue: ## Create release issue on github
	$(IN_VENV) python scripts/bootstrap_history.py --create-release-issue $(RELEASE_CURR)

release-check-metadata: ## check github PR metadata for target release
	$(IN_VENV) python scripts/bootstrap_history.py --check-release $(RELEASE_CURR)

release-check-blocking-issues: ## Check github for release blocking issues
	$(IN_VENV) python scripts/bootstrap_history.py --check-blocking-issues $(RELEASE_CURR)

release-check-blocking-prs: ## Check github for release blocking PRs
	$(IN_VENV) python scripts/bootstrap_history.py --check-blocking-prs $(RELEASE_CURR)

release-bootstrap-history: ## bootstrap history for a new release
	$(IN_VENV) python scripts/bootstrap_history.py --release $(RELEASE_CURR)

update-lint-requirements:
	./lib/galaxy/dependencies/update_lint_requirements.sh

update-dependencies: update-lint-requirements ## update pinned and dev dependencies
	$(IN_VENV) ./lib/galaxy/dependencies/update.sh

node-deps: ## Install NodeJS dependencies.
ifndef YARN
	@echo "Could not find yarn, which is required to build the Galaxy client.\nTo install yarn, please visit \033[0;34mhttps://yarnpkg.com/en/docs/install\033[0m for instructions, and package information for all platforms.\n"
	false;
else
	cd client && yarn install --network-timeout 300000 --check-files
endif
	

client: node-deps ## Rebuild client-side artifacts for local development.
	cd client && yarn run build

client-production: node-deps ## Rebuild client-side artifacts for a production deployment without sourcemaps.
	cd client && yarn run build-production

client-production-maps: node-deps ## Rebuild client-side artifacts for a production deployment with sourcemaps.
	cd client && yarn run build-production-maps

client-format: node-deps ## Reformat client code
	cd client && yarn run prettier

client-watch: node-deps ## A useful target for parallel development building.  See also client-dev-server.
	cd client && yarn run watch

client-dev-server: node-deps ## Starts a webpack dev server for client development (HMR enabled)
	cd client && yarn run webpack-dev-server

client-test: node-deps  ## Run JS unit tests
	cd client && yarn run test

client-eslint: node-deps # Run client linting
	cd client && yarn run eslint

client-format-check: node-deps # Run client formatting check
	cd client && yarn run prettier-check

client-lint: client-eslint client-format-check ## ES lint and check format of client

client-test-watch: client ## Watch and run all client unit tests on changes
	cd client && yarn run jest-watch

serve-selenium-notebooks: ## Serve testing notebooks for Jupyter
	cd lib && export PYTHONPATH=`pwd`; jupyter notebook --notebook-dir=galaxy_test/selenium/jupyter

# Release Targets
release-create-rc: release-ensure-upstream release-ensure-prereqs ## Create a release-candidate branch
	git fetch $(RELEASE_UPSTREAM)
	git checkout -b version-$(RELEASE_CURR).rc1 $(RELEASE_UPSTREAM)/$(RELEASE_SOURCE_BRANCH)
	sed -i -e 's/^VERSION_MAJOR = .*/VERSION_MAJOR = "$(RELEASE_CURR)"/' lib/galaxy/version.py
	sed -i -e 's/^VERSION_MINOR = .*/VERSION_MINOR = "rc1"/' lib/galaxy/version.py
	sed -i -e 's/^RELEASE_CURR:=.*/RELEASE_CURR:=$(RELEASE_CURR)/' Makefile
	sed -i -e 's/^RELEASE_NEXT:=.*/RELEASE_NEXT:=$(RELEASE_NEXT)/' Makefile
	git add lib/galaxy/version.py Makefile
	$(IN_VENV) DEV_RELEASE=1 GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	git add -- packages/
	$(IN_VENV) ./packages/build_scripts/make_all.sh dist
	#git push $(RELEASE_UPSTREAM) release_$(RELEASE_CURR)
	git commit -m "Update version to $(RELEASE_CURR).rc1"
	#git checkout dev
	#
	git checkout -b version-$(RELEASE_NEXT).dev $(RELEASE_UPSTREAM)/$(RELEASE_SOURCE_BRANCH)
	sed -i -e "s/^VERSION_MAJOR = .*/VERSION_MAJOR = \"$(RELEASE_NEXT)\"/" lib/galaxy/version.py
	git add lib/galaxy/version.py
	$(IN_VENV) DEV_RELEASE=1 GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	git add -- packages/
	git commit -m "Update version to $(RELEASE_NEXT).dev"

	-git merge version-$(RELEASE_CURR).rc1
	git checkout --ours -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git add -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git commit -m "Merge branch 'version-$(RELEASE_CURR).rc1' into version-$(RELEASE_NEXT).dev"
	git push $(MY_UPSTREAM) version-$(RELEASE_CURR).rc1:version-$(RELEASE_CURR).rc1
	git push $(MY_UPSTREAM) version-$(RELEASE_NEXT).dev:version-$(RELEASE_NEXT).dev
	git push $(RELEASE_UPSTREAM) $(RELEASE_UPSTREAM)/$(RELEASE_SOURCE_BRANCH):release_$(RELEASE_CURR)
	# TODO: Use hub to automate these PR creations or push directly.
	@echo "Open a PR from version-$(RELEASE_CURR) of your fork to release_$(RELEASE_CURR)"
	@echo "Open a PR from version-$(RELEASE_NEXT).dev of your fork to dev"

release-create-rc-point: release-ensure-upstream release-ensure-prereqs ## Create a release-candidate version
	git checkout release_$(RELEASE_CURR)
	git pull --ff-only $(RELEASE_UPSTREAM) release_$(RELEASE_CURR)
	#git push $(MY_UPSTREAM) release_$(RELEASE_CURR)
	git checkout dev
	git pull --ff-only $(RELEASE_UPSTREAM) dev
	# Test run of merging. If there are conflicts, it will fail here.
	git merge release_$(RELEASE_CURR)
	git checkout release_$(RELEASE_CURR)
	sed -i.bak -e 's/^VERSION_MINOR = .*/VERSION_MINOR = "$(RELEASE_CURR_MINOR_NEXT)"/' lib/galaxy/version.py
	rm -f lib/galaxy/version.py.bak
	git add lib/galaxy/version.py
	$(IN_VENV) DEV_RELEASE=1 GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	git add -- packages/
	$(IN_VENV) ./packages/build_scripts/make_all.sh dist
	git commit -m "Update version to $(RELEASE_CURR).$(RELEASE_CURR_MINOR_NEXT)"

	git checkout dev
	-git merge release_$(RELEASE_CURR)
	git checkout --ours -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git add -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git commit -m "Merge branch 'release_$(RELEASE_CURR)' into dev"
	git push $(RELEASE_UPSTREAM) release_$(RELEASE_CURR):release_$(RELEASE_CURR)
	git push $(RELEASE_UPSTREAM) dev:dev

release-create: release-ensure-upstream release-ensure-prereqs ## Create a release branch
	git checkout master
	git pull --ff-only $(RELEASE_UPSTREAM) master
	git push $(MY_UPSTREAM) master
	git checkout release_$(RELEASE_CURR)
	git pull --ff-only $(RELEASE_UPSTREAM) release_$(RELEASE_CURR)
	#git push $(MY_UPSTREAM) release_$(RELEASE_CURR)
	git checkout dev
	git pull --ff-only $(RELEASE_UPSTREAM) dev
	#git push $(MY_UPSTREAM) dev
	# Test run of merging. If there are conflicts, it will fail here.
	git merge release_$(RELEASE_CURR)
	git checkout release_$(RELEASE_CURR)
	sed -i.bak -e 's/^VERSION_MINOR = .*/VERSION_MINOR = None/' lib/galaxy/version.py
	rm -f lib/galaxy/version.py.bak
	git add lib/galaxy/version.py
	$(IN_VENV) GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	git add -- packages/
	$(IN_VENV) ./packages/build_scripts/make_all.sh dist
	git commit -m "Update version to $(RELEASE_CURR)"
	git tag -m "Tag version $(RELEASE_CURR)" v$(RELEASE_CURR)

	git checkout dev
	-git merge release_$(RELEASE_CURR)
	git checkout --ours -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git add -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git commit -m "Merge branch 'release_$(RELEASE_CURR)' into dev"
	git checkout master
	git merge release_$(RELEASE_CURR)
	git push $(RELEASE_UPSTREAM) release_$(RELEASE_CURR):release_$(RELEASE_CURR)
	git push $(RELEASE_UPSTREAM) dev:dev
	git push $(RELEASE_UPSTREAM) master:master
	git push $(RELEASE_UPSTREAM) --tags

release-create-point: release-ensure-upstream release-ensure-prereqs ## Create a point release
	git checkout master
	git pull --ff-only $(RELEASE_UPSTREAM) master
	git push $(MY_UPSTREAM) master
	git checkout release_$(RELEASE_CURR)
	git pull --ff-only $(RELEASE_UPSTREAM) release_$(RELEASE_CURR)
	#git push $(MY_UPSTREAM) release_$(RELEASE_CURR)
	git checkout $(RELEASE_NEXT_BRANCH)
	git pull --ff-only $(RELEASE_UPSTREAM) $(RELEASE_NEXT_BRANCH)
	#git push $(MY_UPSTREAM) $(RELEASE_NEXT_BRANCH)
	git merge release_$(RELEASE_CURR)
	git checkout release_$(RELEASE_CURR)
	sed -i.bak -e 's/^VERSION_MINOR = .*/VERSION_MINOR = "$(RELEASE_CURR_MINOR_NEXT)"/' lib/galaxy/version.py
	rm -f lib/galaxy/version.py.bak
	git add lib/galaxy/version.py
	$(IN_VENV) GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	git add -- packages/
	$(IN_VENV) ./packages/build_scripts/make_all.sh dist
	git commit -m "Update version to $(RELEASE_CURR).$(RELEASE_CURR_MINOR_NEXT)"
	git tag -m "Tag version $(RELEASE_CURR).$(RELEASE_CURR_MINOR_NEXT)" v$(RELEASE_CURR).$(RELEASE_CURR_MINOR_NEXT)
	git checkout $(RELEASE_NEXT_BRANCH)
	-git merge release_$(RELEASE_CURR)
	git checkout --ours -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git add -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	git commit -m "Merge branch 'release_$(RELEASE_CURR)' into $(RELEASE_NEXT_BRANCH)"
	git checkout master
	git merge release_$(RELEASE_CURR)
	git push $(RELEASE_UPSTREAM) release_$(RELEASE_CURR):release_$(RELEASE_CURR)
	git push $(RELEASE_UPSTREAM) dev:dev
	git push $(RELEASE_UPSTREAM) master:master
	git push $(RELEASE_UPSTREAM) --tags
	git checkout release_$(RELEASE_CURR)

release-create-local: release-ensure-prereqs ## Create a local release
	sed -E -i.bak -e "s/^(VERSION_MINOR = ['\"])([^+]*)[^'\"\]*(['\"])$$/\1\2+$(RELEASE_LOCAL_VERSION)\3/" lib/galaxy/version.py
	sed -E -i.bak -e "s/^(VERSION_MINOR = )None$$/\1\"0+$(RELEASE_LOCAL_VERSION)\"/" lib/galaxy/version.py
	rm -f lib/galaxy/version.py.bak
	$(IN_VENV) DEV_RELEASE=1 GALAXY_RELEASE=1 ./packages/build_scripts/make_all.sh update-version
	#git add -- lib/galaxy/version.py packages/*/galaxy/project_*.py
	$(IN_VENV) ./packages/build_scripts/make_all.sh dist
	#git commit -m "Update version to $(RELEASE_CURR)"
	#git tag -m "Tag version $(RELEASE_CURR)" v$(RELEASE_CURR)

.PHONY: help

help:
	@egrep '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
