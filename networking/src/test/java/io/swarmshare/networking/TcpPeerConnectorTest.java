package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpPeerConnectorTest {

  private ServerSocket server;
  private Thread serverThread;

  @AfterEach
  void tearDown() throws Exception {
    if (server != null && !server.isClosed())
      server.close();
    if (serverThread != null)
      serverThread.interrupt();
  }

  @Test
  void fetchChunk_throwsWhenPeerReturnsNotFound() throws Exception {
    server = new ServerSocket(0);
    serverThread = Thread.startVirtualThread(() -> {
      try (Socket s = server.accept()) {
        var in = s.getInputStream();
        var out = s.getOutputStream();
        int msg = in.read(); // message type
        // reply with NOT_FOUND status + zero-length payload
        out.write(FrameEncoder.STATUS_NOT_FOUND);
        out.write(new byte[] { 0, 0, 0, 0 });
        out.flush();
      } catch (Exception ignored) {
      }
    });

    PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", server.getLocalPort()));
    var connector = new TcpPeerConnector();

    ChunkId id = new ChunkId("deadbeef", 0);

    assertThatThrownBy(() -> connector.fetchChunkAsync(peer, id, 10).join())
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Chunk fetch failed");
  }

  @Test
  void fetchPieceMap_returnsBitSet() throws Exception {
    server = new ServerSocket(0);
    serverThread = Thread.startVirtualThread(() -> {
      try (Socket s = server.accept()) {
        var in = s.getInputStream();
        var out = s.getOutputStream();
        int msg = in.read(); // read message type
        // read hash length and hash - but simplest: consume remaining bytes until EOF
        byte[] buf = new byte[1024];
        while (in.read(buf) > -1) {
          break;
        }

        BitSet bs = new BitSet(16);
        bs.set(1);
        bs.set(5);
        byte[] bytes = bs.toByteArray();
        out.write(FrameEncoder.STATUS_OK);
        int len = bytes.length;
        out.write((len >> 24) & 0xff);
        out.write((len >> 16) & 0xff);
        out.write((len >> 8) & 0xff);
        out.write(len & 0xff);
        if (len > 0)
          out.write(bytes);
        out.flush();
      } catch (Exception ignored) {
      }
    });

    PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", server.getLocalPort()));
    var connector = new TcpPeerConnector();
    BitSet result = connector.fetchPieceMapAsync(peer, "ignored").join();
    assertThat(result.get(1)).isTrue();
    assertThat(result.get(5)).isTrue();
  }
}
