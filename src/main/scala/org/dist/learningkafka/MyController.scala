package org.dist.learningkafka

import java.util.concurrent.atomic.AtomicInteger

import org.dist.simplekafka.common.TopicAndPartition
import org.dist.simplekafka.util.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas, PartitionInfo, PartitionReplicas}

class MyController(val myZookeeperClient: MyZookeeperClient, val brokerId: Int) {
  var currentLeader = -1
  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()


  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }

  def startup(): Unit = {
    myZookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }

  def elect(): Unit = {
    val leaderId = s"${brokerId}"
    try {
      myZookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }
  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ myZookeeperClient.getAllBrokers()
    myZookeeperClient.subscribeTopicChangeListener(new MyTopicChangeHandler(myZookeeperClient,onTopicChange))
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas]
    = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    myZookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList);

  }

  private def getBroker(brokerId: Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }

  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }
}
