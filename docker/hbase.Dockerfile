FROM openjdk:11-jre-slim

ENV HBASE_VERSION=2.5.6
ENV HBASE_HOME=/opt/hbase

RUN apt-get update -qq && apt-get install -y -qq curl bash > /dev/null 2>&1 && \
    curl -sL "https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" | \
    tar -xz -C /opt/ && \
    mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME}

# 配置 HBase standalone 模式
RUN sed -i 's#<configuration>#<configuration>\n\
  <property>\n    <name>hbase.cluster.distributed</name>\n    <value>false</value>\n  </property>\n\
  <property>\n    <name>hbase.rootdir</name>\n    <value>file:///hbase-data</value>\n  </property>\n\
  <property>\n    <name>hbase.zookeeper.property.clientPort</name>\n    <value>2181</value>\n  </property>\n\
  <property>\n    <name>hbase.unsafe.stream.crash.ignore.lock</name>\n    <value>true</value>\n  </property>\n#' ${HBASE_HOME}/conf/hbase-site.xml

RUN mkdir -p /hbase-data && \
    echo "export HBASE_MANAGES_ZK=true" >> ${HBASE_HOME}/conf/hbase-env.sh && \
    echo "export JAVA_HOME=/opt/java/openjdk" >> ${HBASE_HOME}/conf/hbase-env.sh

VOLUME /hbase-data
EXPOSE 16010 9090 2181

CMD ${HBASE_HOME}/bin/start-hbase.sh && \
    ${HBASE_HOME}/bin/hbase-daemon.sh start thrift && \
    tail -f ${HBASE_HOME}/logs/hbase--*.log
