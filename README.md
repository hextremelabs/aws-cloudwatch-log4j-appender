Simple Log4J Appender for AWS Cloudwatch Logs
------------
This appender is intended for use in a Java EE environment that supports Log4J for per-deployment logging (e.g WildFly)

It is often desired that logs be aggregated in specialized logging services as opposed to files because that way
they can be easily made searchable, analyzable and monitorable.

Centralized logging also offers a big value in clustered environments where it may not be clear on which host 
in the cluster an error occurred.

Finally, in an immutable infrastructure environment where clustered containers are setup and torn down on the fly,
having a logging infrastructure that is completely independent of the host container is advantageous.


NOTE: This appender depends on Java EE 7 scheduler for the log synchronization and `com.hextremelabs:quickee` for
CDI-based dynamic config injection. These are very opinionated choices but it should be easy to rip them out of 
the project.


---
Sample usage:
---

*pom.xml*
```
<!-- Add the Maven dependency -->
<dependency>
  <groupId>com.hextremelabs.log4j</groupId>
  <artifactId>cloudwatch-appender</artifactId>
  <version>1.0.Alpha1</version>
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
