package com.hextremelabs.logs.wildfly.cloudwatchhandler

import org.jboss.logmanager.ExtLogRecord
import org.jboss.logmanager.handlers.PeriodicRotatingFileHandler
import java.util.LinkedList
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * @author oladeji
 */
class PeriodicRotatingCloudwatchHandler : PeriodicRotatingFileHandler() {

  lateinit var awsKey: String
  lateinit var awsSecret: String
  lateinit var awsRegion: String
  lateinit var logGroup: String
  lateinit var logStreamPrefix: String

  private lateinit var publisher: CloudwatchPublisher

  private val scheduler = ScheduledThreadPoolExecutor(1)

  init {
    scheduler.scheduleWithFixedDelay({ sync() }, DELAY, DELAY, TimeUnit.SECONDS)
  }

  override fun close() {
    scheduler.shutdown()
    super.close()
  }

  public override fun doPublish(event: ExtLogRecord) {
    if (!event.message.contains(XRAY_ERROR_MESSAGE_TO_SUPPRESS)) {
      LOGS.add(event)
    }
  }

  private fun sync() {
    val pendingLogs = synchronized(LOGS) {
      LinkedList(LOGS)
        .sortedBy(ExtLogRecord::getMillis)
        .also { LOGS.clear() }
    }

    if (!this::publisher.isInitialized) {
      publisher = CloudwatchPublisher(
        awsKey = awsKey,
        awsSecret = awsSecret,
        region = awsRegion,
        logGroup = logGroup,
        logStreamPrefix = logStreamPrefix
      )
    }

    publisher.publishLogs(pendingLogs)
  }

  companion object {
    private val LOGS: ConcurrentLinkedQueue<ExtLogRecord> = ConcurrentLinkedQueue()
    private const val XRAY_ERROR_MESSAGE_TO_SUPPRESS =
      "Failed to begin subsegment named 'AWSLogs': segment cannot be found"
    private const val DELAY: Long = 7
  }
}
