/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.connectors.kafka.source

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.UUID
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

/**
 * Pure unit test (no Testcontainers) proving that [Neo4jQueryTask.start] (and the [resumeFrom] it
 * calls) does not log the configured streaming-property offset value at any level. That value is
 * read from the customer's Neo4j query result (a database column value), so leaking it into the log
 * would expose customer data. The logger is raised to DEBUG in
 * `src/test/resources/simplelogger.properties` so both the INFO resume marker and the DEBUG
 * resumeFrom line are captured.
 */
class Neo4jQueryTaskOffsetLoggingTest {

  @Test
  fun `start must not log the streaming-property offset value at INFO or DEBUG`() {
    // Synthetic, recognizable stored offset value standing in for a customer column value.
    val canaryOffset = 987654321012345L
    // Synthetic literal embedded in the Cypher query. config.partition carries the query text,
    // so a naive partition dump would leak this at INFO; it must not appear there.
    val canaryQueryLiteral = "SECRET_QUERY_LITERAL_4b2e9d"
    val task = Neo4jQueryTask()
    task.initialize(taskContextWithStoredOffset("timestamp", canaryOffset))

    val captured = captureStdErr {
      task.start(
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost:7687",
              Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
              SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
              SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
              SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
              SourceConfiguration.QUERY to
                  "MATCH (n:Test) WHERE n.token = '$canaryQueryLiteral' RETURN n.timestamp AS timestamp",
              SourceConfiguration.QUERY_STREAMING_PROPERTY to "timestamp"))
    }

    // The offset value (a customer column value) must not appear at any level — neither the
    // start() INFO line nor the resumeFrom() DEBUG line (both are captured here).
    assertFalse(
        captured.contains(canaryOffset.toString()),
        "streaming-property offset value must not be logged at INFO or DEBUG, but was found in:\n$captured")
    // The Cypher query text (which can embed literal PII) must not appear at INFO either.
    assertFalse(
        captured.contains(canaryQueryLiteral),
        "Cypher query text must not be logged at INFO, but was found in:\n$captured")
    // Prove the resume INFO branch actually ran (so the assertions above are meaningful).
    assertTrue(
        captured.contains("resuming from stored offset for partition"),
        "expected the resume INFO log to be captured, got:\n$captured")
  }

  private fun captureStdErr(block: () -> Unit): String {
    // slf4j-simple writes to System.err; capture it around the call under test.
    val original = System.err
    val buffer = ByteArrayOutputStream()
    System.setErr(PrintStream(buffer, true))
    try {
      block()
    } finally {
      System.setErr(original)
    }
    return buffer.toString()
  }

  private fun taskContextWithStoredOffset(property: String, value: Long): SourceTaskContext {
    val offsetStorageReader =
        mock<OffsetStorageReader> {
          on { offset(ArgumentMatchers.anyMap<String, Any>()) } doReturn
              mapOf("property" to property, "value" to value)
        }
    return mock<SourceTaskContext> { on { offsetStorageReader() } doReturn offsetStorageReader }
  }
}
