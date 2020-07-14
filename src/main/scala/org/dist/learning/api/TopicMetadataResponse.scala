package org.dist.learning.api

import org.dist.learning.common.ErrorMappings
import org.dist.learning.util.ZkUtils.Broker


case class TopicMetadataResponse(topicsMetadata: Seq[TopicMetadata],
                                 val correlationId: Int)

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMappings.NoError)

case class PartitionMetadata(partitionId: Int,
                             val leader: Option[Broker],
                             replicas: Seq[Broker],
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMappings.NoError)
