FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
# remember to mount the aws redentials!

USER root
ADD https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.9.0-spark_3.1/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar
ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.3/snowflake-jdbc-3.13.3.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar
RUN chmod a+r /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install -r requirements.txt
# USER 185
# COPY data_part_1.json ./ # debugging purposes
COPY ./py_spark/ .
CMD python3 -O script.py