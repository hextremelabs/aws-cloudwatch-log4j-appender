package com.hextremelabs.cloudwatchappender;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.hextremelabs.quickee.configuration.Config;
import com.hextremelabs.quickee.core.Joiner;
import org.apache.log4j.spi.LoggingEvent;
import org.jetbrains.annotations.NotNull;

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

  private AWSLogs client;

  private String nextSequenceToken;

  private String hostIpAddress;

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
  private String logStream;

  @PostConstruct
  public void setup() {
    client = AWSLogsClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsKey, awsSecret)))
        .withRegion(region)
        .build();

    hostIpAddress = getHostIpAddress();

    createLogGroup();
    rotateLogStream();
  }

  private void createLogGroup() {
    final boolean logGroupExists = client.describeLogGroups().getLogGroups()
        .stream().anyMatch(e -> e.getLogGroupName().equals(logGroup));

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

  @NotNull
  private String computeAwsLogStreamName() {
    return logStream + "_" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "_" + hostIpAddress.replace(".", "_");
  }

  private String generateMessage(LoggingEvent event) {
    final StringBuilder result = new StringBuilder()
        .append(formatTimestamp(event.getTimeStamp())).append(' ')
        .append(event.getLevel()).append("  ")
        .append('<').append(hostIpAddress).append("> ")
        .append('[').append(event.getLoggerName()).append("] ")
        .append('(').append(event.getThreadName()).append(")\n")
        .append(event.getRenderedMessage());
    if (event.getThrowableStrRep() != null) {
      result.append("\n").append(Joiner.on("\n").join(event.getThrowableStrRep()));
    }

    return result.toString();
  }

  @NotNull
  private String formatTimestamp(long timestamp) {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date(timestamp));
  }

  @NotNull
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
