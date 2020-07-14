package org.dist.learning

import org.I0Itec.zkclient.ZkClient
import org.dist.common.ZookeeperTestHarness
import org.dist.learning.util.ZKStringSerializer
import org.dist.learning.util.ZkUtils.Broker

class ControllerTest extends ZookeeperTestHarness {
  test("Should elect first server as controller and get all live brokers") {

    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkClient1)
    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkClient2)

//    val listener = new MyBrokerChangeListener(myZookeeperClient1)
//    myZookeeperClient1.subscribeBrokerChangeListener(listener)

    myZookeeperClient1.registerBroker(Broker(1, "10.10.10.01", 9000))
    val controller = new Controller(myZookeeperClient1, 1)
    controller.startup()

    assert(controller.liveBrokers == Set(Broker(1, "10.10.10.01", 9000)))
    assert(controller.currentLeader == 1)

    myZookeeperClient2.registerBroker(Broker(2, "10.10.10.02", 9000))
    val controller1 = new Controller(myZookeeperClient2, 2)
    controller1.startup()

//    assert(controller.liveBrokers == Set(Broker(0, "10.10.10.00", 9000), Broker(1, "10.10.10.01", 9000)))
    assert(controller.currentLeader == 1)

    zkClient1.close()
    assert(controller.currentLeader == 2)
  }
}
