package com.hextremelabs.cloudwatchappender;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Comparator.comparing;

/**
 * @author oladeji
 */
public class CloudWatchAppender extends AppenderSkeleton {

  private static final Queue<LoggingEvent> LOGS = new ConcurrentLinkedQueue<>();

  private static final String XRAY_ERROR_MESSAGE_TO_SUPPRESS = "Failed to begin subsegment named 'AWSLogs': segment cannot be found";

  @Override
  protected void append(LoggingEvent loggingEvent) {
    if (loggingEvent.getMessage().toString().contains(XRAY_ERROR_MESSAGE_TO_SUPPRESS)) {
      return;
    }

    LOGS.add(loggingEvent);
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  static Collection<LoggingEvent> retrieveLogsAndClear() {
    final List<LoggingEvent> events = new LinkedList<>();
    LOGS.removeIf(events::add);
    events.sort(comparing(LoggingEvent::getTimeStamp));
    return events;
  }
}
