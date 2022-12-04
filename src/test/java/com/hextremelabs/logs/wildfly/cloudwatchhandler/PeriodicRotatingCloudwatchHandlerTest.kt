package com.hextremelabs.logs.wildfly.cloudwatchhandler

import org.jboss.logmanager.ExtLogRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.LinkedList
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.logging.Level

/**
 * @author oladeji
 */
internal class PeriodicRotatingCloudwatchHandlerTest {

  @Test
  fun pass() {
  }

  fun retrieveLogsAndClear_multiProducerPopulatedQueue() {
    generate100kLogs()
    val events = emptyList<ExtLogRecord>() // retrieveLogsAndClear()
    Assertions.assertEquals(100000, events.size)
    val iterator = events.iterator()
    var previous = iterator.next().millis
    while (iterator.hasNext()) {
      val current = iterator.next().millis
      Assertions.assertTrue(current >= previous)
      previous = current
    }
  }

  companion object {
    private val POOL = Executors.newFixedThreadPool(100)

    private fun generate100kLogs() {
      val futures: MutableList<Future<*>> = LinkedList()
      for (a in 0..99) {
        futures.add(POOL.submit {
          val sut = PeriodicRotatingCloudwatchHandler()
          for (a1 in 0..999) {
            val event = ExtLogRecord(Level.INFO, "stuff", "Random event" + 1000 * Math.random())
            sut.doPublish(event)
          }
        })
      }

      futures.removeIf {
        try {
          it.get()
          return@removeIf true
        } catch (e1: InterruptedException) {
          Assertions.fail<Any>("Unable to await concurrent WRITE")
          return@removeIf false
        } catch (e1: ExecutionException) {
          Assertions.fail<Any>("Unable to await concurrent WRITE")
          return@removeIf false
        }
      }
    }

    @AfterAll
    @JvmStatic
    fun tearDown() {
      POOL.shutdownNow()
    }
  }
}
