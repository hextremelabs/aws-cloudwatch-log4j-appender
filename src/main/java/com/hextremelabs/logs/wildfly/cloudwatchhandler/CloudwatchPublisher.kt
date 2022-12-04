package com.hextremelabs.logs.wildfly.cloudwatchhandler

import com.hextremelabs.quickee.core.DataHelper.hasBlank
import com.hextremelabs.quickee.core.Joiner
import com.hextremelabs.quickee.time.Clock.getTime
import com.hextremelabs.quickee.time.DateTimeUtil
import org.jboss.logmanager.ExtLogRecord
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException
import java.io.PrintWriter
import java.io.StringWriter
import java.net.NetworkInterface
import java.net.SocketException
import java.text.SimpleDateFormat
import java.util.Random

/**
 * AWS Cloudwatch Logs integration.
 *
 * @author oladeji
 */
class CloudwatchPublisher(
  awsKey: String,
  awsSecret: String,
  region: String,
  private val logGroup: String,
  private val logStreamPrefix: String
) {

  private val client: CloudWatchLogsClient
  private val hostIpAddress: String
  private val uniqueInstanceId: String

  private var nextSequenceToken: String? = null
  private var lastPublishTimestamp: Long = 0

  init {
    val instanceId = try {
      EC2MetadataUtils.getInstanceId()
    } catch (ex: SdkClientException) {
      "EC2-instance-id-not-found"
    }

    uniqueInstanceId = Joiner.on("_").skipNull().join(instanceId, generateRandomId())
    client = CloudWatchLogsClient
      .builder()
      .region(Region.of(region))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsKey, awsSecret)
        )
      )
      .build()
    hostIpAddress = getHostIpAddress()
    createLogGroup()
    rotateLogStream()
  }

  private fun generateRandomId(): String {
    return System.currentTimeMillis().toString() + "-random" + Random().nextInt(100000)
  }

  private fun createLogGroup() {
    var logGroupExists: Boolean
    var nextToken: String? = null
    do {
      val result = client.describeLogGroups(
        DescribeLogGroupsRequest
          .builder()
          .logGroupNamePrefix(logGroup)
          .nextToken(nextToken)
          .build()
      )
      nextToken = result.nextToken()
      logGroupExists = result
        .logGroups()
        .stream()
        .anyMatch { it.logGroupName() == logGroup }
    } while (!logGroupExists && nextToken != null)

    if (!logGroupExists) {
      client.createLogGroup(
        CreateLogGroupRequest
          .builder()
          .logGroupName(logGroup)
          .build()
      )
    }
  }

  private fun rotateLogStream() {
    val logStreamName = computeAwsLogStreamName()
    val logStreamsResult = client.describeLogStreams(
      DescribeLogStreamsRequest
        .builder()
        .logGroupName(logGroup)
        .logStreamNamePrefix(logStreamName)
        .build()
    )

    if (logStreamsResult.logStreams().isNotEmpty()) {
      nextSequenceToken = logStreamsResult
        .logStreams()
        .first()
        .uploadSequenceToken()
      return
    }

    client.createLogStream(
      CreateLogStreamRequest
        .builder()
        .logGroupName(logGroup)
        .logStreamName(logStreamName)
        .build()
    )

    nextSequenceToken = client
      .describeLogStreams(
        DescribeLogStreamsRequest
          .builder()
          .logGroupName(logGroup)
          .logStreamNamePrefix(logStreamName)
          .build()
      )
      .logStreams()
      .first()
      .uploadSequenceToken()
  }

  fun publishLogs(pendingLogs: Collection<ExtLogRecord>) {
    if (hasNotYetPublishedToday()) {
      rotateLogStream()
    }

    val logEventsPayload = pendingLogs
      .filter { !hasBlank(it.formattedMessage) || it.thrown != null }
      .map(::toInputLogEvent)

    val request = PutLogEventsRequest
      .builder()
      .logGroupName(logGroup)
      .logStreamName(computeAwsLogStreamName())
      .logEvents(logEventsPayload)
      .sequenceToken(nextSequenceToken)
      .build()

    lastPublishTimestamp = System.currentTimeMillis()

    nextSequenceToken = try {
      client.putLogEvents(request).nextSequenceToken()
    } catch (ex: Exception) {
      when (ex) {
        is InvalidSequenceTokenException, is ResourceNotFoundException -> {
          // ResourceNotFoundException: When we move to a new day and the log stream for the day is not yet created.

          // InvalidSequenceTokenException: For whatever reason we have messed up the sequence token. This is fine if it's
          // a one-off event. If it keeps reoccurring then we have an extremely rare concurrency issue where this singleton
          // is running in two separate JVM processes on the same EC2 instance started at exactly the same time and seeded
          // with the same random tag; and they're competing for the sequence token. In such a case, kill one of the
          // processes.

          // In either case, let's recreate the log stream if it doesn't exist and reacquire the sequence token.
          rotateLogStream()
          val request2 = PutLogEventsRequest
            .builder()
            .logGroupName(logGroup)
            .logStreamName(computeAwsLogStreamName())
            .logEvents(logEventsPayload)
            .sequenceToken(nextSequenceToken)
            .build()
          client.putLogEvents(request2).nextSequenceToken()
        }
        else -> {
          ex.printStackTrace()
          nextSequenceToken
        }
      }
    }
  }

  private fun hasNotYetPublishedToday() = lastPublishTimestamp < DateTimeUtil.todayStartDate().time

  private fun computeAwsLogStreamName() = Joiner
    .on("_")
    .join(
      logStreamPrefix,
      SimpleDateFormat("yyyy-MM-dd").format(getTime()),
      hostIpAddress,
      uniqueInstanceId
    )

  private fun generateMessage(event: ExtLogRecord): String {
    val loggerName = event.loggerName.let { it.substring(it.lastIndexOf('.') + 1) }
    val result = StringBuilder()
      .append(millisecondUnit(event.millis)).append(" | ")
      .append(event.level.toString(), 0, 4).append(" ")
      .append(loggerName)
      .append(" (").append(event.threadName).append("): ")
      .append(event.formattedMessage)
    if (event.thrown != null) {
      result.append("\n").append(throwableStringRep(event.thrown))
    }
    return result.toString()
  }

  private fun throwableStringRep(throwable: Throwable) = StringWriter()
      .also { throwable.printStackTrace(PrintWriter(it)) }
      .toString()

  private fun millisecondUnit(timestamp: Long) = String.format("%03d", timestamp % 1000)

  private fun getHostIpAddress(): String {
    return try {
      val interfaces = NetworkInterface.getNetworkInterfaces()
      var candidateAddress = ""
      while (interfaces.hasMoreElements()) {
        val addresses = interfaces.nextElement().inetAddresses
        while (addresses.hasMoreElements()) {
          val address = addresses.nextElement()
          var isIpV4 = false
          try {
            candidateAddress = address.hostAddress
            candidateAddress.replace(".", "").toLong()
            isIpV4 = true
          } catch (expected: NumberFormatException) {
          }
          if (!address.isLoopbackAddress && isIpV4) {
            return address.hostAddress
          }
        }
      }
      candidateAddress
    } catch (e: SocketException) {
      "no_addr_" + System.currentTimeMillis()
    }
  }

  private fun toInputLogEvent(event: ExtLogRecord) = InputLogEvent
    .builder()
    .message(generateMessage(event))
    .timestamp(event.millis)
    .build()
}
