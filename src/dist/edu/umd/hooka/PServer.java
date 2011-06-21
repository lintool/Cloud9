package edu.umd.hooka;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.hooka.ttables.TTable;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class PServer implements Runnable {
	
	private TTable ttable;

	public static void main(String[] args) {
		try { PServer v = null; //new PServer(4444);
		ByteBuffer b = ByteBuffer.allocate(20);
		FloatBuffer fb = b.asFloatBuffer();
		fb.put(0.1f); fb.flip();
		System.out.println(fb.position() + "=fpos   bpos=" + b.position());
		Thread t = new Thread(v);
		t.start();
		try {Thread.sleep(100); } catch (Exception e ) {}
		PServerClient psc = new PServerClient("localhost",4444);
		{	int[] e = {201, 202, 203, 1000000, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2,2, 2,2,2,2,2,2,2,2,2,2,2,2,2};
			int[] f = {101, 102, 103, 104, 105, 106,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,3,2,3,5,3,4,2,3,4,5,3,2,5,6,7,12345,34};
			PhrasePair pp = new PhrasePair(new Phrase(e, 0), new Phrase(f, 1));
			psc.query(pp, true);
		}
		//try {Thread.sleep(500); } catch (Exception ex){}
		{int[] e = {201, 202, 203,2,2,2};
		int[] f = {101, 102, 103,6,7,12345,34};
			PhrasePair pp = new PhrasePair(new Phrase(f, 0), new Phrase(e, 1));
			psc.query(pp, true);
			float va = psc.get(2, 34);
			System.out.println(va);
		 }
		try {Thread.sleep(1000); } catch (Exception ex ) {}
		v.stopServer();
		} catch (IOException e) { e.printStackTrace();}
	}
	
	ServerSocketChannel serverChannel;
	public PServer(int port, FileSystem fs, Path ttablePath) throws IOException {
		ttable = new TTable_monolithic_IFAs(fs, ttablePath, true);
		
		serverChannel = ServerSocketChannel.open();
		selector = Selector.open();
		serverChannel.socket().bind (new InetSocketAddress(port));
		serverChannel.configureBlocking (false);
		serverChannel.register (selector, SelectionKey.OP_ACCEPT);		
		System.err.println("PServer initialized on " + InetAddress.getLocalHost() + ":" + port);
	}
	
	public void stopServer() {
		System.err.println("Stopping PServer...");
		try { selector.close(); serverChannel.close(); } catch (Exception e) { System.err.println("Caught " + e); }
	}
	
	Selector selector = null;
	long reqs=0;
	
	public void run() {
		System.err.println("PServer running.");
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
		System.err.println("  " + reqs + " requests processed");
		System.err.println("  " + connections + " connections");
	}
	
	int i = 0;
	int connections = 0;
	HashMap<SelectionKey, ByteBuffer> key2buf = new HashMap<SelectionKey, ByteBuffer>();
	HashMap<SelectionKey, ByteBuffer> key2obuf = new HashMap<SelectionKey, ByteBuffer>();
	static final int READ_BUFFER_SIZE = 35000;
	static final int WRITE_BUFFER_SIZE = 300000;
	
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
		    	  ByteBuffer out_bb = key2obuf.get(key);
		    	  if (in_bb == null) {
		    		  System.err.println("Allocating new buffer!");
		    		  in_bb = ByteBuffer.allocate(READ_BUFFER_SIZE);
		    		  key2buf.put(key, in_bb);
		    		  out_bb = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
		    		  key2obuf.put(key, out_bb);
		    	  }
		    	  SocketChannel sc = (SocketChannel)key.channel();
		    	  int num = sc.read(in_bb);
		    	  if (num == -1) {
		    		  System.out.println("closing");
		    		  key2buf.remove(key);
		    		  sc.close();
		    		  return;
		    	  }
		    	  if (in_bb.position() < 8) return;
		    	  int elen = in_bb.getInt(0);
		    	  if (elen < 1)
		    		  throw new RuntimeException("Elen is out of bounds! elen="+elen);
		    	  int pl = in_bb.position();
		    	  if (elen > pl) {
		    	//	 System.err.println("Haven't read enough! " + elen + " pos="+pl);
		    		  return; // not ready!
		    	  } else {
		    		//  System.err.println("Read enough");
		    	  }
		    	  in_bb.flip();
		    	  in_bb.getInt();
		    	  int fplen = in_bb.getInt();
		    	  IntBuffer ib = in_bb.asIntBuffer();
		    	  int[] ep = new int[fplen];
		    	  ib.get(ep, 0, fplen);
		    	  int[] fp = new int[ib.remaining()];
		    	  ib.get(fp);
		    	  int sz = fp.length * ep.length * Float.SIZE/8;
		    	  out_bb.putInt(sz);
		    	  for (int e : ep)
		    		  for (int f : fp)
		    			  out_bb.putFloat(ttable.get(e, f));
		    	  ++reqs;
		    	  out_bb.flip();
		    	  int x = sc.write(out_bb);
		    	  if (x != sz + 4) {
		    		  System.err.println("Failed to write "+sz+" bytes!  Wrote " + x + " bytes");
		    	  }
		    	//  System.err.println("WROTE " + x + " bytes");
		    	  in_bb.rewind();
		    	  in_bb.limit(READ_BUFFER_SIZE);
		    	  out_bb.rewind();
		    	  out_bb.limit(WRITE_BUFFER_SIZE);
		      } else if (key.isWritable()) {
		    	  throw new IOException("Received writable key - not expecting!");
		      }
	}
}
