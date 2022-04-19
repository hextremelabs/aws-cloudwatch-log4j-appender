package com.hextremelabs.cloudwatchappender;


import com.hextremelabs.quickee.configuration.Config;
import com.hextremelabs.quickee.core.Joiner;
import com.hextremelabs.quickee.time.Clock;
import org.apache.log4j.spi.LoggingEvent;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import javax.annotation.PostConstruct;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.inject.Inject;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Random;

import static com.hextremelabs.cloudwatchappender.CloudWatchAppender.retrieveLogsAndClear;
import static java.net.NetworkInterface.getNetworkInterfaces;
import static java.util.stream.Collectors.toList;
import static javax.ejb.TransactionAttributeType.NOT_SUPPORTED;

/**
 * AWS Cloudwatch Logs integration.
 * Pushes buffered log messages to Cloudwatch every 7 seconds.
 * Creates the configured log group if it doesn't exist and rotates log streams daily.
 *
 * @author oladeji
 */
@Singleton
@Startup
@TransactionAttribute(NOT_SUPPORTED)
public class CloudWatchHandler {

  @Inject
  @Config("aws.key")
  private String awsKey;

  @Inject
  @Config("aws.secret")
  private String awsSecret;

  @Inject
  @Config("cloudwatch.log.region")
  private String region;

  @Inject
  @Config("cloudwatch.log.group")
  private String logGroup;

  @Inject
  @Config("cloudwatch.log.stream")
  private String logStreamPrefix;

  private CloudWatchLogsClient client;

  private String nextSequenceToken;

  private String hostIpAddress;

  private String uniqueInstanceId;

  @PostConstruct
  public void setup() {
    String instanceId = EC2MetadataUtils.getInstanceId();
    if (instanceId == null) instanceId = "EC2-instance-id-not-found";
    uniqueInstanceId = Joiner.on("_").skipNull().join(instanceId, generateRandomId());

    client = CloudWatchLogsClient.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                awsKey,
                awsSecret
            )
        ))
        .build();

    hostIpAddress = getHostIpAddress();

    createLogGroup();
    rotateLogStream();
  }

  private String generateRandomId() {
    return System.currentTimeMillis() + "-random" + new Random().nextInt(100000);
  }

  private void createLogGroup() {
    boolean logGroupExists;
    String nextToken = null;
    do {
      final DescribeLogGroupsRequest request = DescribeLogGroupsRequest.builder()
          .logGroupNamePrefix(logGroup)
          .nextToken(nextToken)
          .build();

      var result = client.describeLogGroups(request);
      nextToken = result.nextToken();
      logGroupExists = result.logGroups()
          .stream().anyMatch(e -> e.logGroupName().equals(logGroup));
    } while (!logGroupExists && nextToken != null);

    if (!logGroupExists) {
      final CreateLogGroupRequest request = CreateLogGroupRequest.builder()
          .logGroupName(logGroup)
          .build();
      client.createLogGroup(request);
    }
  }

  public void rotateLogStream() {
    final String logStreamName = computeAwsLogStreamName();
    final DescribeLogStreamsRequest describeLogStreamsRequest = DescribeLogStreamsRequest.builder()
        .logGroupName(logGroup)
        .logStreamNamePrefix(logStreamName)
        .build();

    final var logStreamsResult = client.describeLogStreams(describeLogStreamsRequest);

    if (!logStreamsResult.logStreams().isEmpty()) {
      nextSequenceToken = logStreamsResult.logStreams().get(0).uploadSequenceToken();
      return;
    }

    final CreateLogStreamRequest request = CreateLogStreamRequest.builder()
        .logGroupName(logGroup)
        .logStreamName(logStreamName)
        .build();

    client.createLogStream(request);

    final DescribeLogStreamsRequest newDescribeLogStreamsRequest = DescribeLogStreamsRequest.builder()
        .logGroupName(logGroup)
        .logStreamNamePrefix(logStreamName)
        .build();
    nextSequenceToken = client.describeLogStreams(newDescribeLogStreamsRequest)
        .logStreams().get(0).uploadSequenceToken();
  }

  @Schedule(hour = "*", minute = "*", second = "*/7", persistent = false)
  public void publishLogs() {
    final Collection<LoggingEvent> pendingLogs = retrieveLogsAndClear();
    if (pendingLogs.isEmpty()) {
      return;
    }

    final PutLogEventsRequest request = PutLogEventsRequest.builder()
        .logGroupName(logGroup)
        .logStreamName(computeAwsLogStreamName())
        .logEvents(pendingLogs.stream()
            .filter(e -> (e.getRenderedMessage() != null && !e.getRenderedMessage().isEmpty())
                || e.getThrowableInformation() != null)
            .map(this::toInputLogEvent)
            .collect(toList()))
        .sequenceToken(nextSequenceToken)
        .build();


    try {
      nextSequenceToken = client.putLogEvents(request).nextSequenceToken();
    } catch (InvalidSequenceTokenException | ResourceNotFoundException ex) {
      // ResourceNotFoundException: When we move to a new day and the log stream for the day is not yet created.

      // InvalidSequenceTokenException: For whatever reason we have messed up the sequence token. This is fine if it's
      // a one-off event. If it keeps reoccurring then we have an extremely rare concurrency issue where this singleton
      // is running in two separate JVM processes on the same EC2 instance started at exactly the same time and seeded
      // with the same random tag; and they're competing for the sequence token. In such a case, kill one of the
      // processes.

      // In either case, let's recreate the log stream if it doesn't exist and reacquire the sequence token.
      rotateLogStream();

      final var request2 = PutLogEventsRequest.builder()
          .logGroupName(logGroup)
          .logStreamName(computeAwsLogStreamName())
          .logEvents(pendingLogs.stream()
              .filter(e -> (e.getRenderedMessage() != null && !e.getRenderedMessage().isEmpty())
                  || e.getThrowableInformation() != null)
              .map(this::toInputLogEvent)
              .collect(toList()))
          .sequenceToken(nextSequenceToken)
          .build();

      nextSequenceToken = client.putLogEvents(request2).nextSequenceToken();
    }
  }

  private String computeAwsLogStreamName() {
    return Joiner.on("_").join(
        logStreamPrefix,
        new SimpleDateFormat("yyyy-MM-dd").format(Clock.getTime()),
        hostIpAddress,
        uniqueInstanceId
    );
  }

  private String generateMessage(LoggingEvent event) {
    String loggerName = event.getLoggerName();
    loggerName = loggerName.substring(loggerName.lastIndexOf('.') + 1);

    final StringBuilder result = new StringBuilder()
        .append(millisecondUnit(event.getTimeStamp())).append(" | ")
        .append(event.getLevel().toString(), 0, 4).append(" ")
        .append(loggerName)
        .append(" (").append(event.getThreadName()).append("): ")
        .append(event.getRenderedMessage());
    if (event.getThrowableStrRep() != null) {
      result.append("\n").append(String.join("\n", event.getThrowableStrRep()));
    }

    return result.toString();
  }

  private String millisecondUnit(long timestamp) {
    return String.format("%03d", timestamp % 1000);
  }

  private String getHostIpAddress() {
    final Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = getNetworkInterfaces();

      String candidateAddress = "";
      while (interfaces.hasMoreElements()) {
        final Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();

        while (addresses.hasMoreElements()) {
          final InetAddress address = addresses.nextElement();
          boolean isIpV4 = false;
          try {
            candidateAddress = address.getHostAddress();
            Long.parseLong(candidateAddress.replace(".", ""));
            isIpV4 = true;
          } catch (NumberFormatException expected) {
          }

          if (!address.isLoopbackAddress() && isIpV4) {
            return address.getHostAddress();
          }
        }
      }

      return candidateAddress;
    } catch (SocketException e) {
      return "no_addr_" + System.currentTimeMillis();
    }
  }

  private InputLogEvent toInputLogEvent(LoggingEvent event) {
    final InputLogEvent result = InputLogEvent.builder()
        .message(generateMessage(event))
        .timestamp(event.getTimeStamp())
        .build();
    return result;
  }
}
