package org.dist.learning

import org.dist.common.ZookeeperTestHarness
import org.dist.learning.util.ZkUtils.Broker

class CreateTopicCommandZookeeperTest extends ZookeeperTestHarness{
  test(testName = "should do partition to broker assignment") {
    val myZookeeperClient = new MyZookeeperClient(zkClient)
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.00", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.01", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.02", 8000))

    val createCommand = new CreateTopicCommand(myZookeeperClient)
    createCommand.createTopic("transactions", 3, 1)

    val topics = myZookeeperClient.getAllTopics()
    assert(topics.size == 1)

    val partitionAssignments = topics("transactions")
    assert(partitionAssignments.size == 1)


  }

}
