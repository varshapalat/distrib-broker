package org.dist.learningkafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))

    assert(3 == myZookeeperClient.getAllBrokers().size)
    assertResult(Broker(0, "10.10.10.10", 8000))( myZookeeperClient.getBrokerInfo(0))
  }

  test("master node subscribes and listens if any of the brokers fails") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkClient1)

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkClient2)

    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient3 = new MyZookeeperClient(zkClient3)

    val listener = new MyBrokerChangeListener(myZookeeperClient1)
    myZookeeperClient1.subscribeBrokerChangeListener(listener)

    myZookeeperClient1.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient2.registerBroker(Broker(1, "10.10.10.11", 8000))
    myZookeeperClient3.registerBroker(Broker(2, "10.10.10.12", 8000))

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    zkClient2.close()
    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 2
    }, "Waiting for all brokers to get added", 1000)

    zkClient3.close()
    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 1000)
  }
}
