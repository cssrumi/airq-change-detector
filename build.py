import subprocess
import xml.etree.ElementTree as ET

NS = '{http://maven.apache.org/POM/4.0.0}'

pom_tree = ET.parse('pom.xml')
pom_root = pom_tree.getroot()
version = pom_root.find(NS + 'version').text
group_id = pom_root.find(NS + 'groupId').text
company = group_id.split('.')[-1]
artifact_id = pom_root.find(NS + 'artifactId').text

properties = pom_root.find(NS+'properties')
common_version = properties.find(NS + 'common.version').text

tag = company + '/' + artifact_id + ':' + version
common_tag = 'COMMON_TAG=' + common_version

print("building {} using common {}".format(tag, common_version))

build = subprocess.Popen(['docker', 'build', '-f', 'src/main/docker/Dockerfile.multistage', '-t', tag, '--build-arg', common_tag, '.'])
build.wait()
