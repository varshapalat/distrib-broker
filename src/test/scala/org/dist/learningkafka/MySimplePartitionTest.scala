package org.dist.learningkafka

import org.dist.common.TestUtils
import org.scalatest.FunSuite

class MySimplePartitionTest extends FunSuite {
  test("should append messages to file and return offset") {
    val p = new MySimplePartition("topic1", 0, TestUtils.tempDir())
    assert(p.logFile.exists())

    val offset = p.append("k1", "m1".getBytes)
    assert(offset == 1)

    val messages = p.read(offset)
    assert(messages.size == 1)
    assert(messages(0) == "m1")
  }
}
