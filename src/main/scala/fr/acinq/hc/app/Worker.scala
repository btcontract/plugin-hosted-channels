package fr.acinq.hc.app

import fr.acinq.eclair.io._
import fr.acinq.hc.app.Worker._
import fr.acinq.hc.app.channel._
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, Props, Terminated}
import scala.concurrent.ExecutionContext.Implicits.global
import fr.acinq.hc.app.db.HostedChannelsDb
import com.google.common.collect.HashBiMap
import fr.acinq.hc.app.network.HostedSync
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.bitcoin.ByteVector32
import fr.acinq.hc.app.wire.Codecs
import scala.collection.mutable
import grizzled.slf4j.Logging
import fr.acinq.eclair
import scodec.Attempt


object Worker {
  case object HCPeerConnected

  case object HCPeerDisconnected

  case object TickClearIpAntiSpam { val label = "TickClearIpAntiSpam" }

  case object TickRemoveIdleChannels { val label = "TickRemoveIdleChannels" }

  case class ClientChannels(channels: Seq[HC_DATA_ESTABLISHED] = Nil)

  val notFound: CMDResFailure = CMDResFailure("HC with remote node is not found")

  val isHidden: CMDResFailure = CMDResFailure("HC with remote node is hidden")
}

class Worker(kit: eclair.Kit, hostedSync: ActorRef, preimageCatcher: ActorRef, channelsDb: HostedChannelsDb, vals: Vals) extends Actor with Logging { me =>
  context.system.scheduler.scheduleWithFixedDelay(60.minutes, 60.minutes, self, Worker.TickClearIpAntiSpam)
  context.system.scheduler.scheduleWithFixedDelay(2.days, 2.days, self, TickRemoveIdleChannels)

  context.system.eventStream.subscribe(channel = classOf[UnknownMessageReceived], subscriber = self)
  context.system.eventStream.subscribe(channel = classOf[PeerDisconnected], subscriber = self)
  context.system.eventStream.subscribe(channel = classOf[PeerConnected], subscriber = self)

  val inMemoryHostedChannels: HashBiMap[PublicKey, ActorRef] = HashBiMap.create[PublicKey, ActorRef]

  val ipAntiSpam: mutable.Map[Array[Byte], Int] = mutable.Map.empty withDefaultValue 0

  override def receive: Receive = {
    case systemMessage: PeerConnected if systemMessage.connectionInfo.remoteInit.features.hasPluginFeature(HCFeature.plugin) =>
      HC.remoteNode2Connection addOne systemMessage.nodeId -> PeerConnectedWrapNormal(systemMessage)
      val refOpt = Option(inMemoryHostedChannels get systemMessage.nodeId)
      refOpt.foreach(_ |> HCPeerDisconnected |> HCPeerConnected)

    case systemMessage: PeerDisconnected =>
      HC.remoteNode2Connection subtractOne systemMessage.nodeId
      val refOpt = Option(inMemoryHostedChannels get systemMessage.nodeId)
      refOpt.foreach(_ ! HCPeerDisconnected)

    case Worker.TickClearIpAntiSpam => ipAntiSpam.clear

    case peerMsg @ UnknownMessageReceived(_, nodeId, message, _) =>
      if (HC.preimageQueryTags contains message.tag) preimageCatcher ! peerMsg
      else if (HC.announceTags contains message.tag) hostedSync ! peerMsg
      else if (HC.hostedMessageTags contains message.tag) {
        Tuple3(Codecs decodeHostedMessage message, HC.remoteNode2Connection get nodeId, inMemoryHostedChannels get nodeId) match {
          case (_: Attempt.Failure, _, _) => logger.info(s"PLGN PHC, Hosted message decoding fail, messageTag=${message.tag}, peer=$nodeId")
          case (_, None, _) => logger.info(s"PLGN PHC, no live connection found for hosted channel message=${message.tag}, peer=$nodeId")
          case (Attempt.Successful(_: ReplyPublicHostedChannelsEnd), Some(wrap), _) => hostedSync ! HostedSync.GotAllSyncFrom(wrap)
          case (Attempt.Successful(_: QueryPublicHostedChannels), Some(wrap), _) => hostedSync ! HostedSync.SendSyncTo(wrap)

          // Special handling for InvokeHostedChannel: if chan exists neither in memory nor in db, then this is a new chan request and anti-spam rules apply
          case (Attempt.Successful(invoke: InvokeHostedChannel), Some(wrap), null) => restore(guardSpawn(nodeId, wrap, invoke), Tools.none, _ |> HCPeerConnected |> invoke)(nodeId)
          case (Attempt.Successful(_: HostedChannelMessage), _, null) => logger.info(s"PLGN PHC, no target for HostedMessage, messageTag=${message.tag}, peer=$nodeId")
          case (Attempt.Successful(hosted: HostedChannelMessage), _, channelRef) => channelRef ! hosted
        }
      } else if (HC.chanIdMessageTags contains message.tag) {
        Tuple3(Codecs decodeHasChanIdMessage message, HC.remoteNode2Connection get nodeId, inMemoryHostedChannels get nodeId) match {
          case (_: Attempt.Failure, _, _) => logger.info(s"PLGN PHC, HasChannelId message decoding fail, tag=${message.tag}, peer=$nodeId")
          case (_, None, _) => logger.info(s"PLGN PHC, no live connection found for containing channel id message=${message.tag}, peer=$nodeId")
          case (Attempt.Successful(error: eclair.wire.Error), _, null) => restore(Tools.none, Tools.none, _ |> HCPeerConnected |> error)(nodeId)
          case (_, _, null) => logger.info(s"PLGN PHC, no target for HasChannelIdMessage, tag=${message.tag}, peer=$nodeId")
          case (Attempt.Successful(msg), _, channelRef) => channelRef ! msg
        }
      }

    case cmd: HC_CMD_LOCAL_INVOKE =>
      val isConnected = HC.remoteNode2Connection.contains(cmd.remoteNodeId)
      val isInDb = channelsDb.getChannelByRemoteNodeId(cmd.remoteNodeId).nonEmpty
      val isInMemory = Option(inMemoryHostedChannels get cmd.remoteNodeId).nonEmpty
      if (kit.nodeParams.nodeId == cmd.remoteNodeId) sender ! CMDResFailure("HC with itself is prohibited")
      else if (isInMemory || isInDb) sender ! CMDResFailure("HC with remote peer already exists")
      else if (!isConnected) sender ! CMDResFailure("Not yet connected to remote peer")
      else spawnChannel(cmd.remoteNodeId) |> HCPeerConnected |> cmd

    case cmd: HC_CMD_RESTORE =>
      val isInDb = channelsDb.getChannelByRemoteNodeId(cmd.remoteNodeId).nonEmpty
      val isInMemory = Option(inMemoryHostedChannels get cmd.remoteNodeId).nonEmpty
      if (kit.nodeParams.nodeId == cmd.remoteNodeId) sender ! CMDResFailure("HC with itself is prohibited")
      else if (isInMemory || isInDb) sender ! CMDResFailure("HC with remote peer already exists")
      else spawnChannel(cmd.remoteNodeId) ! cmd

    // Peer may be disconnected when commands are issued
    // Channel must be able to handle command in any state

    case cmd: HasRemoteNodeIdHostedCommand =>
      Option(inMemoryHostedChannels get cmd.remoteNodeId) match {
        case None => restore(sender ! notFound, sender ! isHidden, _ |> cmd)(cmd.remoteNodeId)
        case Some(ref) => ref forward cmd
      }

    case Terminated(channelRef) => inMemoryHostedChannels.inverse.remove(channelRef)

    case TickRemoveIdleChannels => inMemoryHostedChannels.values.forEach(_ ! TickRemoveIdleChannels)

    case cc: ClientChannels =>
      for (channelData <- cc.channels) {
        val nodeId = channelData.commitments.remoteNodeId
        kit.switchboard ! Peer.Connect(nodeId, None)
        spawnPreparedChannel(channelData)
      }
  }

  def spawnChannel(nodeId: PublicKey): ActorRef = {
    val spawnedChannelProps = Props(classOf[HostedChannel], kit, nodeId, channelsDb, hostedSync, vals)
    val channelRef = context watch context.actorOf(spawnedChannelProps)
    inMemoryHostedChannels.put(nodeId, channelRef)
    channelRef
  }

  def guardSpawn(nodeId: PublicKey, wrap: PeerConnectedWrap, invoke: InvokeHostedChannel): Unit = {
    // Spawn new HC requested by remote peer if that peer is in our override map (special handling) or if there are not too many such requests from remote IP
    if (vals.hcOverrideMap.contains(nodeId) || ipAntiSpam(wrap.remoteIp) < vals.maxNewChansPerIpPerHour) spawnChannel(nodeId) |> HCPeerConnected |> invoke
    else wrap sendHasChannelIdMsg eclair.wire.Error(ByteVector32.Zeroes, ErrorCodes.ERR_HOSTED_CHANNEL_DENIED)
    // Record this request for anti-spam
    ipAntiSpam(wrap.remoteIp) += 1
  }

  def spawnPreparedChannel(data: HC_DATA_ESTABLISHED): ActorRef = {
    val channel = spawnChannel(data.commitments.remoteNodeId)
    channel ! data
    channel
  }

  def restore(onNotFound: => Unit, onHidden: => Unit, onFound: ActorRef => Unit)(nodeId: PublicKey): Unit =
    channelsDb.getChannelByRemoteNodeId(nodeId) collect {
      case (data, true) => onFound(me spawnPreparedChannel data)
      case (_, false) => onHidden
    } getOrElse onNotFound

  implicit class MultiSender(channel: ActorRef) {
    def |>(message: Any): MultiSender = {
      channel forward message
      this
    }
  }
}
