![Build Status](https://github.com/hextremelabs/aws-cloudwatch-log4j-appender/workflows/Deploy%20Build/badge.svg)

Simple Log4J Appender for AWS Cloudwatch Logs
------------
This appender is intended for use in a Java EE environment that supports Log4J for per-deployment logging (e.g WildFly)

It is often desired that logs be aggregated in specialized logging services as opposed to files because that way
they can be easily made searchable, analyzable and monitorable.

Centralized logging also offers a big value in clustered environments where it may not be clear on which host 
in the cluster an error occurred. Finally, in an immutable infrastructure environment where containers are setup and torn down on the fly, having a logging infrastructure that is completely independent of the host container is advantageous.


NOTE: This appender depends on Java EE scheduler for the log synchronization and `com.hextremelabs:quickee` for
CDI-based dynamic config injection. These are very opinionated choices and it is easy to fork this repo and rip them out if not desired.


---
Sample usage:
---

*pom.xml*
```
<!-- Add the Maven dependency -->
<dependency>
  <groupId>com.hextremelabs.log4j</groupId>
  <artifactId>cloudwatch-appender</artifactId>
  <version>1.0.Alpha5</version>
</dependency>
```

*log4j.properties*
```
# Configure the logger
log4j.rootLogger=INFO, cloudwatch

log4j.appender.cloudwatch=com.hextremelabs.cloudwatchappender.CloudWatchAppender
log4j.appender.cloudwatch.layout=org.apache.log4j.PatternLayout
log4j.appender.cloudwatch.layout.ConversionPattern=%d [%t] %-5p %c %x - %m%n
```

*quickee config keys*
```
aws.key                 # Access key for your AWS IAM user
aws.secret              # Secret key for your AWS IAM user
cloudwatch.log.region   # AWS region where your logs reside (e.g us-west-1)
cloudwatch.log.group    # Log group name (e.g my-app-prod)
cloudwatch.log.stream   # Prefix for log stream names, rotated daily by date (e.g my-app)
```
