package nanodb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
//
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
/**
NanoDBWorker, the counterpart of NanoDBConnect (or Client), spawned/started by NanoDBServer
@author Joe T. Schwarz (c)
*/
public class NanoDBWorker implements Runnable {
  /**
  contructor
  @param soc   SocketChannel
  @param nanoMgr  NanoDBManager
  */
  public NanoDBWorker(SocketChannel soc, NanoDBManager nanoMgr) {
    this.soc = soc;
    this.nanoMgr = nanoMgr;
  }
  //
  public void run() {
    try {
      soc.socket().setTcpNoDelay(true);
      soc.socket().setSendBufferSize(65536);
      soc.socket().setReceiveBufferSize(65536);
      ByteBuffer bbuf = ByteBuffer.allocate(65536);
      String userID = String.format("ID%08X", System.nanoTime());
      ByteArrayOutputStream bao = new ByteArrayOutputStream(65536);
      while(!nanoMgr.closed) {
        bao.reset();
        int dl = 0;
        do {
          bbuf.clear();
          dl = soc.read(bbuf);
          bao.write(bbuf.flip().array(), 0, dl);
        } while (dl >= 65536);
        byte[] buf, bb = bao.toByteArray();
        //
        // bb format: 1st byte: cmd, 2 bytes: dbName length, 2 bytes: key length, 4 bytes: data Length, dbName, key, data
        // bb format: 1st byte: cmd, 2 bytes: dbName length, 2 bytes: key/data length, dbName, keye/data
        // bb format: 1st byte: cmd, 2 bytes: key length, key or name
        // bb format: 1st byte: cmd
        //
        // returned bb[0] = 0y00: OK, 0x01: error
        // by OK: up bb[1]... replied data as Object as List or byte[]
        //
        dl = (((int)bb[1] & 0xFF) << 8)|((int)bb[2] & 0xFF);
        int nl, kl, cmd = (int)bb[0] & 0xFF;
        switch (cmd) {
          case 0: // open(dbName, charsetName)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.open(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 1: // close(dbName)
            soc.write(ByteBuffer.wrap(nanoMgr.close(userID, new String(bb, 3, dl))));
            break;
          case 2: // getKeys(dbName)
            soc.write(ByteBuffer.wrap(nanoMgr.getKeys(new String(bb, 3, dl))));
            break;
          case 3: // autoCommit(dbName, true) or autoCommit(dbName, false)
            soc.write(ByteBuffer.wrap(nanoMgr.autoCommit(new String(bb, 5, dl), bb[5+dl] == (byte)0x00)));
            break;
          case 4: // isAutoCommit(dbName)
            soc.write(ByteBuffer.wrap(nanoMgr.isAutoCommit(new String(bb, 3, dl))));
            break;
          case 5: // lock(dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.lock(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 6: // unlock(dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.unlock(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 7: // isLocked(dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.isLocked(new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 8: // isExisted(dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.isExisted(new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 9: // isKeyDeleted(dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.isKeyDeleted(new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 10: // readObject(userID, dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.readObject(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 11: // addObject(userID, dbName, key, byte[])
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            nl = (((int)bb[5] & 0xFF) << 24)|(((int)bb[6] & 0xFF) << 16)|(((int)bb[7] & 0xFF) << 8)|((int)bb[8] & 0xFF);
            buf = new byte[nl];
            System.arraycopy(bb, 9+dl+kl, buf, 0, nl);
            soc.write(ByteBuffer.wrap(nanoMgr.addObject(userID, new String(bb, 9, dl), new String(bb, 9+dl, kl), buf)));
            break;
          case 12: // deleteObject(userID, dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.deleteObject(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 13: // updateObject(userID, dbName, key, byte[])
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            nl = (((int)bb[5] & 0xFF) << 24)|(((int)bb[6] & 0xFF) << 16)|(((int)bb[7] & 0xFF) << 8)|((int)bb[8] & 0xFF);
            buf = new byte[nl];
            System.arraycopy(bb, 9+dl+kl, buf, 0, nl);
            soc.write(ByteBuffer.wrap(nanoMgr.updateObject(userID, new String(bb, 9, dl), new String(bb, 9+dl, kl), buf)));
            break;
          case 14: // commit(userID, dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.commit(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 15: // commitAll(userID, dbName)
            soc.write(ByteBuffer.wrap(nanoMgr.commitAll(userID, new String(bb, 3, dl))));
            break;
          case 16: // rollback(userID, dbName, key)
            kl = (((int)bb[3] & 0xFF) << 8)|((int)bb[4] & 0xFF);
            soc.write(ByteBuffer.wrap(nanoMgr.rollback(userID, new String(bb, 5, dl), new String(bb, 5+dl, kl))));
            break;
          case 17: // rollbackAll(userID, dbName)
            soc.write(ByteBuffer.wrap(nanoMgr.rollbackAll(userID, new String(bb, 3, dl))));
            break;
          case 18: // disconnect()
            soc.write(ByteBuffer.wrap(nanoMgr.disconnect(userID)));
            soc.close();
            return;
        }
      }
    } catch (Exception ex) {
      if (!nanoMgr.closed) try {
        soc.close();
      } catch (Exception e) { }
    }
  }
  private SocketChannel soc;
  private NanoDBManager nanoMgr;
}
