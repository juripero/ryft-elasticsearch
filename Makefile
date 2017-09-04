# support for VERSION AND GITHASH environment variables
PACKAGE_NAME=ryft-elastic241-integration
HINT=${PACKAGE_NAME}/debian

all: package

ifeq (${VERSION},)
  VERSION=$(shell git describe --tags)
endif
ifeq (${GITHASH},)
  GITHASH=$(shell git log -1 --format='%h')
endif

.PHONY: version
version:
	@echo "Version: ${VERSION}"
	@echo "githash: ${GITHASH}"

RYFT_PLUGIN_FOLDER=../ryft-elastic-plugin/target/releases
DEBNAME=${PACKAGE_NAME}_${VERSION}_all.deb
DESTDIR=.build

.PHONY: package
package: control
	@echo "[${HINT}]: building deb package..."
	@dpkg-deb --build "${DESTDIR}" "${DEBNAME}"
	@rm -rf "${DESTDIR}"

# create DEBIAN control file
CONTROLFILE=${DESTDIR}/DEBIAN/control
.PHONY: control
control: template
	@echo "[${HINT}]: creating CONTROL file..."
	@echo "Package: ${PACKAGE_NAME}" > "${CONTROLFILE}"
	@echo "Version: ${VERSION}" >> "${CONTROLFILE}"
	@echo "Section: custom" >> "${CONTROLFILE}"
	@echo "Priority: optional" >> "${CONTROLFILE}"
	@echo "Architecture: all" >> "${CONTROLFILE}"
	@echo "Essential: no" >> "${CONTROLFILE}"
	@echo "Installed-Size: $$(du -k -s ${DESTDIR} | awk '{print $$1}')" >> "${CONTROLFILE}"
	@echo "Maintainer: www.ryft.com" >> "${CONTROLFILE}"
	@echo "Description: RYFT integration with elasticsearch 2.4.1" >> "${CONTROLFILE}"
	@echo "Depends: elasticsearch (= 2.4.1)" >> "${CONTROLFILE}"
	@echo "Suggests: kibana, ryft-server" >> "${CONTROLFILE}"

# initialize template
.PHONY: template
template: build
	@echo "[${HINT}]: initializing package template..."
	@rm -rf ${DESTDIR}
	@mkdir -p ${DESTDIR}/DEBIAN
	@mkdir -p ${DESTDIR}/usr/share/elasticsearch/lib/
	@mkdir -p ${DESTDIR}/usr/share/elasticsearch/plugins/ryft-elastic-plugin/
	@unzip $(shell ls ryft-elastic-plugin/target/releases/ryft-elastic-plugin*.zip | head -n 1) -d ${DESTDIR}/usr/share/elasticsearch/plugins/ryft-elastic-plugin/
	@mv ${DESTDIR}/usr/share/elasticsearch/plugins/ryft-elastic-plugin/lucene-codecs-*.jar ${DESTDIR}/usr/share/elasticsearch/lib/
	@cp ryft-elastic-codec/target/ryft-elastic-codec*.jar ${DESTDIR}/usr/share/elasticsearch/lib/
	
.PHONY: build
build:
	@echo "[${HINT}]: building plugin..."
	@mvn clean install -pl '!ryft-elastic-integration-test' -DskipTests