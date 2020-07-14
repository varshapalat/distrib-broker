package org.dist.learning



  class CreateTopicCommand(zookeeperClient:MyZookeeperClient, partitionAssigner:ReplicaAssignmentStrategy = new ReplicaAssignmentStrategy()) {

    def createTopic(topicName:String, noOfPartitions:Int, replicationFactor:Int) = {
      val brokerIds = zookeeperClient.getAllBrokerIds()
      //get list of brokers
      //assign replicas to partition
      val partitionReplicas: Set[PartitionReplicas] = assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
      // register topic with partition assignments to zookeeper
      zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)

    }

}
