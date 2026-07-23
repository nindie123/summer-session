FROM docker-flink-job:latest
COPY docker/flink-submit.sh /opt/flink/jobs/submit.sh
RUN chmod +x /opt/flink/jobs/submit.sh
CMD ["/opt/flink/jobs/submit.sh"]
