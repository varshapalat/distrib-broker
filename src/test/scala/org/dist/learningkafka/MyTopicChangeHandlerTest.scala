package org.dist.learningkafka

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.dist.simplekafka.util.ZkUtils.Broker

class MyTopicChangeHandlerTest extends ZookeeperTestHarness{
  class TestContext {
    var replicas:Seq[PartitionReplicas] = List()
    def leaderAndIsr(topicName : String, replicas: Seq[PartitionReplicas]):Unit = {
      this.replicas = replicas
    }
  }

  test("should register for topic change and get replica assignments") {
    val myZookeeperClient = new MyZookeeperClient(zkClient)
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.00", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.01", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.02", 8000))

    val createTopicCommand = new MyCreateTopicCommand(myZookeeperClient)
    val testContext = new TestContext
    val topicChangeListener = new MyTopicChangeHandler(myZookeeperClient, testContext.leaderAndIsr)
    myZookeeperClient.subscribeTopicChangeListener(topicChangeListener)
    createTopicCommand.createTopic("topic1", 2, 2)

    TestUtils.waitUntilTrue(() => {
      testContext.replicas.size >0
    }, "Waiting for topic metadata")
  }
}
