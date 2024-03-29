/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import joptsimple.OptionParser
import kafka.cluster.Broker
import kafka.message.{MessageSet, MessageAndOffset, ByteBufferMessageSet}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import kafka.client.ClientUtils
import java.util.regex.{PatternSyntaxException, Pattern}
import kafka.api._
import java.text.SimpleDateFormat
import java.util.Date
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.utils._
import kafka.consumer.{ConsumerConfig, Whitelist, SimpleConsumer}

/**
 *  For verifying the consistency among replicas.
 *
 *  1. start a fetcher on every broker.
 *  2. each fetcher does the following
 *    2.1 issues fetch request
 *    2.2 puts the fetched result in a shared buffer
 *    2.3 waits for all other fetchers to finish step 2.2
 *    2.4 one of the fetchers verifies the consistency of fetched results among replicas
 *
 *  The consistency verification is up to the high watermark. The tool reports the
 *  max lag between the verified offset and the high watermark among all partitions.
 *
 *  If a broker goes down, the verification of the partitions on that broker is delayed
 *  until the broker is up again.
 *
 * Caveats:
 * 1. The tools needs all brokers to be up at startup time.
 * 2. The tool doesn't handle out of range offsets.
 */

object ReplicaVerificationTool extends Logging {
  val clientId= "replicaVerificationTool"
  val dateFormatString = "yyyy-MM-dd HH:mm:ss,SSS"
  val dateFormat = new SimpleDateFormat(dateFormatString)

  def getCurrentTimeString() = {
    ReplicaVerificationTool.dateFormat.format(new Date(SystemTime.milliseconds))
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                         .withRequiredArg
                         .describedAs("hostname:port,...,hostname:port")
                         .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The fetch size of each request.")
                         .withRequiredArg
                         .describedAs("bytes")
                         .ofType(classOf[java.lang.Integer])
                         .defaultsTo(ConsumerConfig.FetchSize)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                         .withRequiredArg
                         .describedAs("ms")
                         .ofType(classOf[java.lang.Integer])
                         .defaultsTo(1000)
    val topicWhiteListOpt = parser.accepts("topic-white-list", "White list of topics to verify replica consistency. Defaults to all topics.")
                         .withRequiredArg
                         .describedAs("Java regex (String)")
                         .ofType(classOf[String])
                         .defaultsTo(".*")
    val initialOffsetTimeOpt = parser.accepts("time", "Timestamp for getting the initial offsets.")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val reportIntervalOpt = parser.accepts("report-interval-ms", "The reporting interval.")
                         .withRequiredArg
                         .describedAs("ms")
                         .ofType(classOf[java.lang.Long])
                         .defaultsTo(30 * 1000L)


    val options = parser.parse(args : _*)
    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt)

    val regex = options.valueOf(topicWhiteListOpt)
    val topicWhiteListFiler = new Whitelist(regex)

    try {
      Pattern.compile(regex)
    }
    catch {
      case e: PatternSyntaxException =>
        throw new RuntimeException(regex + " is an invalid regex.")
    }

    val fetchSize = options.valueOf(fetchSizeOpt).intValue
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue
    val initialOffsetTime = options.valueOf(initialOffsetTimeOpt).longValue
    val reportInterval = options.valueOf(reportIntervalOpt).longValue
    // getting topic metadata
    info("Getting topic metatdata...")
    val metadataTargetBrokers = ClientUtils.parseBrokerList(options.valueOf(brokerListOpt))
    val topicsMetadataResponse = ClientUtils.fetchTopicMetadata(Set[String](), metadataTargetBrokers, clientId, maxWaitMs)
    val brokerMap = topicsMetadataResponse.extractBrokers(topicsMetadataResponse.topicsMetadata)
    val filteredTopicMetadata = topicsMetadataResponse.topicsMetadata.filter(
        topicMetadata => if (topicWhiteListFiler.isTopicAllowed(topicMetadata.topic)) true else false
    )
    val topicPartitionReplicaList: Seq[TopicPartitionReplica] = filteredTopicMetadata.flatMap(
      topicMetadataResponse =>
        topicMetadataResponse.partitionsMetadata.flatMap(
          partitionMetadata =>
            partitionMetadata.replicas.map(broker =>
              TopicPartitionReplica(topic = topicMetadataResponse.topic, partitionId = partitionMetadata.partitionId, replicaId = broker.id))
        )
    )
    debug("Selected topic partitions: " + topicPartitionReplicaList)
    val topicAndPartitionsPerBroker: Map[Int, Seq[TopicAndPartition]] = topicPartitionReplicaList.groupBy(_.replicaId)
      .map { case (brokerId, partitions) =>
               brokerId -> partitions.map { case partition => new TopicAndPartition(partition.topic, partition.partitionId) } }
    debug("Topic partitions per broker: " + topicAndPartitionsPerBroker)
    val expectedReplicasPerTopicAndPartition: Map[TopicAndPartition, Int] =
          topicPartitionReplicaList.groupBy(replica => new TopicAndPartition(replica.topic, replica.partitionId))
          .map { case (topicAndPartition, replicaSet) => topicAndPartition -> replicaSet.size }
    debug("Expected replicas per topic partition: " + expectedReplicasPerTopicAndPartition)
    val leadersPerBroker: Map[Int, Seq[TopicAndPartition]] = filteredTopicMetadata.flatMap(
      topicMetadataResponse =>
        topicMetadataResponse.partitionsMetadata.map(
          partitionMetadata =>
            (new TopicAndPartition(topicMetadataResponse.topic, partitionMetadata.partitionId), partitionMetadata.leader.get.id))
    ).groupBy(_._2)
     .mapValues(topicAndPartitionAndLeaderIds => topicAndPartitionAndLeaderIds.map {
        case(topicAndPartition, leaderId) => topicAndPartition })
    debug("Leaders per broker: " + leadersPerBroker)

    val replicaBuffer = new ReplicaBuffer(expectedReplicasPerTopicAndPartition,
                                          leadersPerBroker,
                                          topicAndPartitionsPerBroker.size,
                                          brokerMap,
                                          initialOffsetTime,
                                          reportInterval)
    // create all replica fetcher threads
    val verificationBrokerId = topicAndPartitionsPerBroker.head._1
    val fetcherThreads: Iterable[ReplicaFetcher] = topicAndPartitionsPerBroker.map {
      case (brokerId, topicAndPartitions) =>
        new ReplicaFetcher(name = "ReplicaFetcher-" + brokerId,
                           sourceBroker = brokerMap(brokerId),
                           topicAndPartitions = topicAndPartitions,
                           replicaBuffer = replicaBuffer,
                           socketTimeout = 30000,
                           socketBufferSize = 256000,
                           fetchSize = fetchSize,
                           maxWait = maxWaitMs,
                           minBytes = 1,
                           doVerification = (brokerId == verificationBrokerId))
    }

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        info("Stopping all fetchers")
        fetcherThreads.foreach(_.shutdown())
      }
    })
    fetcherThreads.foreach(_.start())
    println(ReplicaVerificationTool.getCurrentTimeString() + ": verification process is started.")

  }
}

private case class TopicPartitionReplica(topic: String,  partitionId: Int,  replicaId: Int)

private case class ReplicaAndMessageIterator(replicaId: Int, iterator: Iterator[MessageAndOffset])

private case class MessageInfo(replicaId: Int, offset: Long, nextOffset: Long, checksum: Long)

private class ReplicaBuffer(expectedReplicasPerTopicAndPartition: Map[TopicAndPartition, Int],
                            leadersPerBroker: Map[Int, Seq[TopicAndPartition]],
                            expectedNumFetchers: Int,
                            brokerMap: Map[Int, Broker],
                            initialOffsetTime: Long,
                            reportInterval: Long) extends Logging {
  private val fetchOffsetMap = new Pool[TopicAndPartition, Long]
  private val messageSetCache = new Pool[TopicAndPartition, Pool[Int, FetchResponsePartitionData]]
  private val fetcherBarrier = new AtomicReference(new CountDownLatch(expectedNumFetchers))
  private val verificationBarrier = new AtomicReference(new CountDownLatch(1))
  @volatile private var lastReportTime = SystemTime.milliseconds
  private var maxLag: Long = -1L
  private var offsetWithMaxLag: Long = -1L
  private var maxLagTopicAndPartition: TopicAndPartition = null
  initialize()

  def createNewFetcherBarrier() {
    fetcherBarrier.set(new CountDownLatch(expectedNumFetchers))
  }

  def getFetcherBarrier() = fetcherBarrier.get()

  def createNewVerificationBarrier() {
    verificationBarrier.set(new CountDownLatch(1))
  }

  def getVerificationBarrier() = verificationBarrier.get()

  private def initialize() {
    for (topicAndPartition <- expectedReplicasPerTopicAndPartition.keySet)
      messageSetCache.put(topicAndPartition, new Pool[Int, FetchResponsePartitionData])
    setInitialOffsets()
  }

  private def offsetResponseStringWithError(offsetResponse: OffsetResponse): String = {
    offsetResponse.partitionErrorAndOffsets.filter {
      case (topicAndPartition, partitionOffsetsResponse) => partitionOffsetsResponse.error != ErrorMapping.NoError
    }.mkString
  }

  private def setInitialOffsets() {
    for ((brokerId, topicAndPartitions) <- leadersPerBroker) {
      val broker = brokerMap(brokerId)
      val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 100000, ReplicaVerificationTool.clientId)
      val initialOffsetMap: Map[TopicAndPartition, PartitionOffsetRequestInfo] =
        topicAndPartitions.map(topicAndPartition => topicAndPartition -> PartitionOffsetRequestInfo(initialOffsetTime, 1)).toMap
      val offsetRequest = OffsetRequest(initialOffsetMap)
      val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
      assert(!offsetResponse.hasError, offsetResponseStringWithError(offsetResponse))
      offsetResponse.partitionErrorAndOffsets.foreach{
        case (topicAndPartition, partitionOffsetResponse) =>
          fetchOffsetMap.put(topicAndPartition, partitionOffsetResponse.offsets.head)
      }
    }
  }

  def addFetchedData(topicAndPartition: TopicAndPartition, replicaId: Int, partitionData: FetchResponsePartitionData) {
    messageSetCache.get(topicAndPartition).put(replicaId, partitionData)
  }

  def getOffset(topicAndPartition: TopicAndPartition) = {
    fetchOffsetMap.get(topicAndPartition)
  }

  def verifyCheckSum() {
    debug("Begin verification")
    maxLag = -1L
    for ((topicAndPartition, fetchResponsePerReplica) <- messageSetCache) {
      debug("Verifying " + topicAndPartition)
      assert(fetchResponsePerReplica.size == expectedReplicasPerTopicAndPartition(topicAndPartition),
            "fetched " + fetchResponsePerReplica.size + " replicas for " + topicAndPartition + ", but expected "
            + expectedReplicasPerTopicAndPartition(topicAndPartition) + " replicas")
      val messageIteratorMap = fetchResponsePerReplica.map {
        case(replicaId, fetchResponse) =>
          replicaId -> fetchResponse.messages.asInstanceOf[ByteBufferMessageSet].shallowIterator}
      val maxHw = fetchResponsePerReplica.values.map(_.hw).max

      // Iterate one message at a time from every replica, until high watermark is reached.
      var isMessageInAllReplicas = true
      while (isMessageInAllReplicas) {
        var messageInfoFromFirstReplicaOpt: Option[MessageInfo] = None
        for ( (replicaId, messageIterator) <- messageIteratorMap) {
          if (messageIterator.hasNext) {
            val messageAndOffset = messageIterator.next()

            // only verify up to the high watermark
            if (messageAndOffset.offset >= fetchResponsePerReplica.get(replicaId).hw)
              isMessageInAllReplicas = false
            else {
              messageInfoFromFirstReplicaOpt match {
                case None =>
                  messageInfoFromFirstReplicaOpt = Some(
                    MessageInfo(replicaId, messageAndOffset.offset,messageAndOffset.nextOffset, messageAndOffset.message.checksum))
                case Some(messageInfoFromFirstReplica) =>
                  if (messageInfoFromFirstReplica.offset != messageAndOffset.offset) {
                    println(ReplicaVerificationTool.getCurrentTimeString + ": partition " + topicAndPartition
                      + ": replica " + messageInfoFromFirstReplica.replicaId + "'s offset "
                      + messageInfoFromFirstReplica.offset + " doesn't match replica "
                      + replicaId + "'s offset " + messageAndOffset.offset)
                    System.exit(1)
                  }
                  if (messageInfoFromFirstReplica.checksum != messageAndOffset.message.checksum)
                    println(ReplicaVerificationTool.getCurrentTimeString + ": partition "
                      + topicAndPartition + " has unmatched checksum at offset " + messageAndOffset.offset + "; replica "
                      + messageInfoFromFirstReplica.replicaId + "'s checksum " + messageInfoFromFirstReplica.checksum
                      + "; replica " + replicaId + "'s checksum " + messageAndOffset.message.checksum)
              }
            }
          } else
            isMessageInAllReplicas = false
        }
        if (isMessageInAllReplicas) {
          val nextOffset = messageInfoFromFirstReplicaOpt.get.nextOffset
          fetchOffsetMap.put(topicAndPartition, nextOffset)
          debug(expectedReplicasPerTopicAndPartition(topicAndPartition) + " replicas match at offset " +
                nextOffset + " for " + topicAndPartition)
        }
      }
      if (maxHw - fetchOffsetMap.get(topicAndPartition) > maxLag) {
        offsetWithMaxLag = fetchOffsetMap.get(topicAndPartition)
        maxLag = maxHw - offsetWithMaxLag
        maxLagTopicAndPartition = topicAndPartition
      }
      fetchResponsePerReplica.clear()
    }
    val currentTimeMs = SystemTime.milliseconds
    if (currentTimeMs - lastReportTime > reportInterval) {
      println(ReplicaVerificationTool.dateFormat.format(new Date(currentTimeMs)) + ": max lag is "
        + maxLag + " for partition " + maxLagTopicAndPartition + " at offset " + offsetWithMaxLag
        + " among " + messageSetCache.size + " paritions")
      lastReportTime = currentTimeMs
    }
  }
}

private class ReplicaFetcher(name: String, sourceBroker: Broker, topicAndPartitions: Iterable[TopicAndPartition],
                             replicaBuffer: ReplicaBuffer, socketTimeout: Int, socketBufferSize: Int,
                             fetchSize: Int, maxWait: Int, minBytes: Int, doVerification: Boolean)
  extends ShutdownableThread(name) {
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, ReplicaVerificationTool.clientId)
  val fetchRequestBuilder = new FetchRequestBuilder().
          clientId(ReplicaVerificationTool.clientId).
          replicaId(Request.DebuggingConsumerId).
          maxWait(maxWait).
          minBytes(minBytes)

  override def doWork() {

    val fetcherBarrier = replicaBuffer.getFetcherBarrier()
    val verificationBarrier = replicaBuffer.getVerificationBarrier()

    for (topicAndPartition <- topicAndPartitions)
      fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
        replicaBuffer.getOffset(topicAndPartition), fetchSize)

    val fetchRequest = fetchRequestBuilder.build()
    debug("Issuing fetch request " + fetchRequest)

    var response: FetchResponse = null
    try {
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (!isRunning.get)
          throw t
    }

    if (response != null) {
      response.data.foreach {
        case(topicAndPartition, partitionData) =>
          replicaBuffer.addFetchedData(topicAndPartition, sourceBroker.id, partitionData)
      }
    } else {
      for (topicAndPartition <- topicAndPartitions)
        replicaBuffer.addFetchedData(topicAndPartition, sourceBroker.id, new FetchResponsePartitionData(messages = MessageSet.Empty))
    }

    fetcherBarrier.countDown()
    debug("Done fetching")

    // wait for all fetchers to finish
    fetcherBarrier.await()
    debug("Ready for verification")

    // one of the fetchers will do the verification
    if (doVerification) {
      debug("Do verification")
      replicaBuffer.verifyCheckSum()
      replicaBuffer.createNewFetcherBarrier()
      replicaBuffer.createNewVerificationBarrier()
      debug("Created new barrier")
      verificationBarrier.countDown()
    }

    verificationBarrier.await()
    debug("Done verification")
  }
}