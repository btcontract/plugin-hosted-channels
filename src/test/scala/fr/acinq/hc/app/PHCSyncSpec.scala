package fr.acinq.hc.app

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.testkit.{TestFSMRef, TestProbe}
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.bitcoin.{Block, ByteVector64, Crypto}
import fr.acinq.eclair._
import fr.acinq.eclair.channel.Channel.OutgoingMessage
import fr.acinq.eclair.io.{ConnectionInfo, PeerConnected, UnknownMessageReceived}
import fr.acinq.eclair.router.Router.Data
import fr.acinq.eclair.router.{Announcements, BaseRouterSpec, Router, SyncProgress}
import fr.acinq.eclair.wire.UnknownMessage
import fr.acinq.hc.app.db.HostedUpdatesDb
import fr.acinq.hc.app.network.HostedSync.{GotAllSyncFrom, SendSyncTo, TickSendGossip}
import fr.acinq.hc.app.network._
import fr.acinq.hc.app.wire.Codecs


class PHCSyncSpec extends BaseRouterSpec {
  private def createPeer(nodeId: PublicKey)(implicit system: ActorSystem) = {
    val connection = TestProbe()
    val peer = TestProbe()
    val connectionInfo = ConnectionInfo(new InetSocketAddress("192.168.0.101", 9807), connection.ref, null, null)
    val wrap = PeerConnectedWrapNormal(PeerConnected(peer.ref, nodeId, connectionInfo))
    (peer, connection, wrap)
  }

  test("Hosted sync and gossip") { fixture =>
    HCTestUtils.resetEntireDatabase(Config.db)
    val probe = TestProbe()
    val config = Config.vals.phcConfig.copy(minNormalChans = 1, maxPerNode = 1)
    val (kit, _) = HCTestUtils.testKit(TestConstants.Alice.nodeParams)(system)
    val syncActor = TestFSMRef(new HostedSync(kit.copy(router = fixture.router), new HostedUpdatesDb(Config.db), config))
    awaitCond(syncActor.stateName == WAIT_FOR_ROUTER_DATA)
    // Router has finished synchronization
    syncActor ! SyncProgress(1D)
    // No PHC enabled peers are connected yet
    awaitCond(syncActor.stateName == WAIT_FOR_PHC_SYNC)
    val privatePeer = createPeer(randomKey.publicKey)._3
    // Private HC peer is connected
    HC.remoteNode2Connection addOne privatePeer.info.nodeId -> privatePeer
    syncActor ! HostedSync.SyncFromPHCPeers
    awaitCond(syncActor.stateName == WAIT_FOR_PHC_SYNC)

    probe.send(fixture.router, Router.GetRouterData)
    val routerData = probe.expectMsgType[Data]

    val (peer, _, wrap) = createPeer(routerData.nodes.keys.head)
    // PHC peer is connected (can be seen in normal graph), synchronizing
    HC.remoteNode2Connection addOne wrap.info.nodeId -> wrap
    syncActor ! HostedSync.SyncFromPHCPeers
    assert(peer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.HC_QUERY_PUBLIC_HOSTED_CHANNELS_TAG)
    awaitCond(syncActor.stateName == DOING_PHC_SYNC)

    val shortId = Tools.hostedShortChanId(a.value, b.value)
    val randomSig: ByteVector64 = Crypto.sign(randomBytes32, randomKey)
    val announce = Announcements.makeChannelAnnouncement(Block.RegtestGenesisBlock.hash, shortId, a, b, a, b, randomSig, randomSig, randomSig, randomSig)
    val update1 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_a, b, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)
    val update2 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_b, a, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)

    syncActor ! UnknownMessageReceived(null, null, Codecs.toUnknownAnnounceMessage(announce, isGossip = false), null)
    syncActor ! UnknownMessageReceived(null, null, Codecs.toUnknownAnnounceMessage(update1, isGossip = false), null)
    syncActor ! UnknownMessageReceived(null, null, Codecs.toUnknownAnnounceMessage(update2, isGossip = false), null)

    {
      // Another peer connects and asks for we->them sync
      val (syncPeer, _, syncReceiver) = createPeer(randomKey.publicKey)
      syncActor ! SendSyncTo(syncReceiver)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_ANNOUNCE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
    }

    // Finished synchronizing
    syncActor ! GotAllSyncFrom(wrap)
    awaitCond(syncActor.stateName == OPERATIONAL)
    val secondPublicNode = routerData.nodes.keys.tail.head
    val thirdPublicNode = routerData.nodes.keys.tail.tail.head

    {
      // Getting invalid gossip (wrong announce format)
      val shortId = Tools.hostedShortChanId(c.value, a.value)
      val randomSig: ByteVector64 = Crypto.sign(randomBytes32, randomKey)
      val announce = Announcements.makeChannelAnnouncement(Block.RegtestGenesisBlock.hash, shortId, c, a, b, d, randomSig, randomSig, randomSig, randomSig) // nodeId != bitcoinKey
      val update1 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_c, a, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)
      val update2 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_a, c, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)

      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(announce, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update1, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update2, isGossip = true), null)
    }

    {
      // Getting gossip
      val shortId = Tools.hostedShortChanId(c.value, d.value)
      val randomSig: ByteVector64 = Crypto.sign(randomBytes32, randomKey)
      val announce = Announcements.makeChannelAnnouncement(Block.RegtestGenesisBlock.hash, shortId, c, d, c, d, randomSig, randomSig, randomSig, randomSig)
      val update1 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_c, d, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)
      val update2 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_d, c, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)

      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(announce, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update1, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update2, isGossip = true), null)
    }

    {
      // Getting invalid gossip (too many PHC)
      val shortId = Tools.hostedShortChanId(c.value, d.value)
      val randomSig: ByteVector64 = Crypto.sign(randomBytes32, randomKey)
      val announce = Announcements.makeChannelAnnouncement(Block.RegtestGenesisBlock.hash, shortId, c, a, c, a, randomSig, randomSig, randomSig, randomSig)
      val update1 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_a, c, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)
      val update2 = Announcements.makeChannelUpdate(Block.RegtestGenesisBlock.hash, priv_c, a, shortId, CltvExpiryDelta(5), 7000000.msat, 50000.msat, 100, config.minCapacity, enable = true)

      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(announce, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update1, isGossip = true), null)
      syncActor ! UnknownMessageReceived(null, secondPublicNode, Codecs.toUnknownAnnounceMessage(update2, isGossip = true), null)
    }

    {
      // Broadcasting collected gossip
      val (gossipPeer1, _, gossipReceiver1) = createPeer(secondPublicNode)
      val (gossipPeer2, _, gossipReceiver2) = createPeer(thirdPublicNode)
      HC.remoteNode2Connection.clear()
      HC.remoteNode2Connection addOne gossipReceiver1.info.nodeId -> gossipReceiver1
      HC.remoteNode2Connection addOne gossipReceiver2.info.nodeId -> gossipReceiver2
      syncActor ! TickSendGossip
      gossipPeer1.expectNoMessage() // We have seen gossip from this one, so no updates to it
      // Only 3 messages, invalid gossip discarded
      assert(gossipPeer2.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_ANNOUNCE_GOSSIP_TAG)
      assert(gossipPeer2.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_GOSSIP_TAG)
      assert(gossipPeer2.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_GOSSIP_TAG)
      syncActor ! TickSendGossip
      gossipPeer1.expectNoMessage()
      gossipPeer2.expectNoMessage()
    }

    // Another sync cycle, network is recreated from db in the end
    HC.remoteNode2Connection.clear()
    HC.remoteNode2Connection addOne wrap.info.nodeId -> wrap
    syncActor ! HostedSync.SyncFromPHCPeers
    awaitCond(syncActor.stateName == DOING_PHC_SYNC)
    syncActor ! GotAllSyncFrom(wrap)
    awaitCond(syncActor.stateName == OPERATIONAL)

    {
      // Sending out updates obtained through sync and gossip
      val (syncPeer, _, syncReceiver) = createPeer(randomKey.publicKey)
      syncActor ! SendSyncTo(syncReceiver)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_ANNOUNCE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_ANNOUNCE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.PHC_UPDATE_SYNC_TAG)
      assert(syncPeer.expectMsgType[OutgoingMessage].msg.asInstanceOf[UnknownMessage].tag == HC.HC_REPLY_PUBLIC_HOSTED_CHANNELS_END_TAG)
    }
  }
}
