package org.dist.learningkafka

import org.dist.common.ZookeeperTestHarness
import org.dist.simplekafka.util.ZkUtils.Broker

class MyCreateTopicCommandTest extends ZookeeperTestHarness {
  test("should create persistent path for topic with  topic partition assignments in zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient)
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.01", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.02", 8001))
    myZookeeperClient.registerBroker(Broker(3, "10.10.10.03", 8002))

    val createCommand = new MyCreateTopicCommand(myZookeeperClient)
    createCommand.createTopic("transactions", 2, 3)

    val topics = myZookeeperClient.getAllTopics()
    assert(topics.size == 1)

    val partitionAssignments = topics("transactions")
    assert(partitionAssignments.size == 2)
    partitionAssignments.foreach(p => assert(p.brokerIds.size == 3))
  }
}
