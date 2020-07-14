package org.dist.learning

import java.util.concurrent.atomic.AtomicInteger

import org.dist.learning.util.ZkUtils.Broker

class Controller(val zookeeperClient: MyZookeeperClient, val brokerId: Int) {

  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = -1

  def startup(): Unit = {
    elect()
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    //getalltopics and partition assignments
//    zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, onTopicChange))

    zookeeperClient.subscribeBrokerChangeListener(new MyBrokerChangeListener(zookeeperClient))
  }
}
