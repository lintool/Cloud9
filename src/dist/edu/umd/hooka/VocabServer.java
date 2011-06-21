package edu.umd.hooka;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

public class VocabServer implements Runnable {

	public static void main(String[] args) {
		try { VocabServer v = new VocabServer(4444);
		Thread t = new Thread(v);
		t.start();
		try {Thread.sleep(10000); } catch (Exception e ) {}
		v.stopServer();
		} catch (IOException e) { e.printStackTrace();}
	}
	
	ServerSocketChannel serverChannel;
	public VocabServer(int port) throws IOException {
		serverChannel = ServerSocketChannel.open();
		selector = Selector.open();
		serverChannel.socket().bind (new InetSocketAddress(port));
		serverChannel.configureBlocking (false);
		serverChannel.register (selector, SelectionKey.OP_ACCEPT);		
		System.err.println("Vocab server initialized on port " + port);
	}
	
	public void stopServer() {
		System.err.println("Stopping server...");
		try { selector.close(); } catch (Exception e) { System.err.println("Caught " + e); }
	}
	
	Selector selector = null;
	
	Text t = new Text();
	VocabularyWritable v = new VocabularyWritable();

	public void run() {
		System.err.println("Vocab server running...");
		while (true) {
			try {
				selector.select();
			} catch (IOException e) {
				System.err.println("Caught exception in select()");
				e.printStackTrace();
				break;
			}

			if (selector.isOpen() == false) break;
		   Iterator<SelectionKey> it = selector.selectedKeys().iterator();

		   while (it.hasNext()) {
		      SelectionKey key = it.next();
		      try {
		        processSelectionKey(key);
		      } catch (IOException e) {
		    	  key.cancel();
		    	  System.err.println("Caught exception handling selection key. Key cancelled");
		      }
		      it.remove();
		   }
		}
		System.err.println("Server exiting.");
		System.err.println("  " + (v.size()-1) + " types processed");
		System.err.println("  " + connections + " connections");
	}
	
	ByteBuffer out_bb = ByteBuffer.allocate(4);
	int i = 0;
	int connections = 0;
	HashMap<SelectionKey, ByteBuffer> key2buf = new HashMap<SelectionKey, ByteBuffer>();
	
	protected void processSelectionKey(SelectionKey key) throws IOException {
	      if (key.isAcceptable()) {
		         ServerSocketChannel server = (ServerSocketChannel) key.channel();
		         SocketChannel channel = server.accept();
		         if (channel == null) return;

		         channel.configureBlocking (false);
		         channel.register (selector, SelectionKey.OP_READ);
		         connections++;
		      } else if (key.isReadable()) {
		    	  ByteBuffer in_bb = key2buf.get(key);
		    	  if (in_bb == null) {
		    		  System.err.println("Allocating new buffer!");
		    		  in_bb = ByteBuffer.allocate(2048);
		    		  key2buf.put(key, in_bb);
		    	  }
		    	  SocketChannel sc = (SocketChannel)key.channel();
		    	  int num = sc.read(in_bb);
		    	  if (num == -1) {
		    		  System.out.println("closing");
		    		  key2buf.remove(key);
		    		  sc.close();
		    		  return;
		    	  }
		    	  int elen = in_bb.get(0);
		    	  if (elen < 1)
		    		  throw new RuntimeException("Elen is out of bounds! elen="+elen);
		    	  int pl = in_bb.position();
		    	  if (elen + 3 > pl) {
		    		 // System.err.println("Haven't read enough! " + elen + " pos="+pl);
		    		  return; // not ready!
		    	  } else {
		    		 // System.err.println("Read enough");
		    	  }
		    	  in_bb.flip();
		    	  t.set(in_bb.array(), 2, in_bb.limit() - 2);
		    	  i = v.addOrGet(t.toString());
		    	  //System.err.println(t.toString());
		    	  /*if (t.getLength() > 15)
		    		  throw new RuntimeException("Too long!!");*/
		    	  out_bb.putInt(i);
		    	  out_bb.flip();
		    	  int x = sc.write(out_bb);
		    	  if (x != 4)
		    		  throw new IOException("Failed to write 4 bytes!");
		    	  in_bb.rewind();
		    	  in_bb.limit(2048);
		    	  out_bb.rewind();
		      } else if (key.isWritable()) {
		    	  throw new IOException("Received writable socket - not expecting!");
		      }
	}
}
