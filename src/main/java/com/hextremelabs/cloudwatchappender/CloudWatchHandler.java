package com.hextremelabs.cloudwatchappender;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.util.EC2MetadataUtils;
import com.hextremelabs.quickee.configuration.Config;
import com.hextremelabs.quickee.core.Joiner;
import org.apache.log4j.spi.LoggingEvent;

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
import java.util.Date;
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

  private AWSLogs client;

  private String nextSequenceToken;

  private String hostIpAddress;

  private String uniqueInstanceId;

  @PostConstruct
  public void setup() {
    try {
      uniqueInstanceId = EC2MetadataUtils.getInstanceId();
      if (uniqueInstanceId == null) assignRandomUID();
    } catch (Exception ex) {
      assignRandomUID();
    }

    client = AWSLogsClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsKey, awsSecret)))
        .withRegion(region)
        .build();

    hostIpAddress = getHostIpAddress();

    createLogGroup();
    rotateLogStream();
  }

  private void assignRandomUID() {
    uniqueInstanceId = System.currentTimeMillis() + "_random" + new Random().nextInt(1000);
  }

  private void createLogGroup() {
    boolean logGroupExists;
    String nextToken = null;
    do {
      final DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();
      request.setLogGroupNamePrefix(logGroup);
      request.setNextToken(nextToken);

      final DescribeLogGroupsResult result = client.describeLogGroups(request);
      nextToken = result.getNextToken();
      logGroupExists = result.getLogGroups()
          .stream().anyMatch(e -> e.getLogGroupName().equals(logGroup));
    } while (!logGroupExists && nextToken != null);

    if (!logGroupExists) {
      final CreateLogGroupRequest request = new CreateLogGroupRequest();
      request.setLogGroupName(logGroup);
      client.createLogGroup(request);
    }
  }

  @Schedule(persistent = false)
  public void rotateLogStream() {
    final String logStreamName = computeAwsLogStreamName();
    final DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest();
    describeLogStreamsRequest.setLogGroupName(logGroup);
    describeLogStreamsRequest.setLogStreamNamePrefix(logStreamName);
    final DescribeLogStreamsResult logStreamsResult = client.describeLogStreams(describeLogStreamsRequest);

    if (logStreamsResult.getLogStreams().isEmpty()) {
      final CreateLogStreamRequest request = new CreateLogStreamRequest();
      request.setLogGroupName(logGroup);
      request.setLogStreamName(logStreamName);
      client.createLogStream(request);

      final DescribeLogStreamsRequest newDescribeLogStreamsRequest = new DescribeLogStreamsRequest();
      newDescribeLogStreamsRequest.setLogGroupName(logGroup);
      newDescribeLogStreamsRequest.setLogStreamNamePrefix(logStreamName);
      nextSequenceToken = client.describeLogStreams(newDescribeLogStreamsRequest)
          .getLogStreams().get(0).getUploadSequenceToken();
    } else {
      nextSequenceToken = logStreamsResult.getLogStreams().get(0).getUploadSequenceToken();
    }
  }

  @Schedule(hour = "*", minute = "*", second = "*/7", persistent = false)
  public void publishLogs() {
    final Collection<LoggingEvent> pendingLogs = retrieveLogsAndClear();
    if (pendingLogs.isEmpty()) {
      return;
    }

    final PutLogEventsRequest request = new PutLogEventsRequest();
    request.setLogGroupName(logGroup);
    request.setLogStreamName(computeAwsLogStreamName());
    request.setLogEvents(pendingLogs.stream()
        .filter(e -> (e.getRenderedMessage() != null && !e.getRenderedMessage().isEmpty())
            || e.getThrowableInformation() != null)
        .map(this::toInputLogEvent)
        .collect(toList()));
    request.setSequenceToken(nextSequenceToken);
    nextSequenceToken = client.putLogEvents(request).getNextSequenceToken();
  }

  private String computeAwsLogStreamName() {
    return Joiner.on("_").join(logStreamPrefix, new SimpleDateFormat("yyyy-MM-dd").format(new Date()),
        uniqueInstanceId);
  }

  private String generateMessage(LoggingEvent event) {
    String loggerName = event.getLoggerName();
    loggerName = loggerName.substring(loggerName.lastIndexOf('.') + 1);

    final StringBuilder result = new StringBuilder()
        .append(millisecondUnit(event.getTimeStamp())).append(" | ")
        .append(event.getLevel().toString(), 0, 4).append(" ")
        .append(loggerName).append(": ")
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
    final InputLogEvent result = new InputLogEvent();
    result.setMessage(generateMessage(event));
    result.setTimestamp(event.getTimeStamp());
    return result;
  }
}
