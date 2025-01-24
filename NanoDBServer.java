package nanodb;

import java.awt.*;
import java.net.*;
import javax.swing.*;
import java.awt.event.*;
import java.util.concurrent.*;
//
import java.io.*;
import java.nio.channels.*;
/**
 An implemented NanoDBServer using ServerSocketChannel.
 @author Joe T. Schwarz (c)
*/
public class NanoDBServer extends JFrame {
  //
  public static void main(String... argv) throws Exception {
    UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
    new NanoDBServer( );
  }
  /**
  Constructor. This is the base for a customized NanoDB server
  @param hostPort String, format: hostName:portNumber
  @param path String, Directory path of NanoDB files
  @exception Exception thrown by JAVA
  */
  public NanoDBServer( ) throws Exception {
    setTitle("NanoDBServer");
    setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
    ExecutorService pool = Executors.newFixedThreadPool(1024);
    //
    JTextField jpath = new JTextField(System.getProperty("user.dir"));
    JTextField jhost = new JTextField("localhost");
    JTextField jport = new JTextField("9999");
    JTextField jLim  = new JTextField("1");
    jLim.setPreferredSize(new Dimension(50, 25));
    JLabel lab = new JLabel("is NOT running");
    jport.addKeyListener(new KeyAdapter() {
      public void keyTyped(KeyEvent e) {
        char c = e.getKeyChar();
        if (c < '0' || c > '9') e.consume(); 
      }
    });
    jLim.addKeyListener(new KeyAdapter() {
      public void keyTyped(KeyEvent e) {
        char c = e.getKeyChar();
        if (c < '0' || c > '9') e.consume();
        try {
          int lim = Integer.parseInt(jLim.getText());
          if (lim > 1024) jLim.setText("1024");
          else if (lim < 1) e.consume();
        } catch (Exception ex) {
          jLim.setText("1");
        }
      }
    });
    //
    JButton start = new JButton("START");
    start.addActionListener(a -> {
      if ("EXIT".equals(start.getText())) {
        nanoMgr.closed = true;
        if (!running) {
          running = false;
          try {
            dbSvr.close();
          } catch (Exception ex) { }
        }
        pool.shutdownNow();
        System.exit(0);
      }
      jhost.setEnabled(false);
      jport.setEnabled(false);
      jpath.setEnabled(false);
      start.setText("EXIT");
      pool.execute(() -> {
        String host = jhost.getText().trim();
        int port = Integer.parseInt(jport.getText());
        jport.setEnabled(false);
        boolean go = false;
        try {
          dbSvr = ServerSocketChannel.open();
          dbSvr.socket().bind(new InetSocketAddress(host, port));
          dbSvr.setOption(StandardSocketOptions.SO_RCVBUF, 65536);
          jhost.setEnabled(false); jport.setEnabled(false);jpath.setEnabled(false);jLim.setEnabled(false);
          //
          go = true;
          lab.setText("is running...");
          nanoMgr = new NanoDBManager(jpath.getText().trim(), 0x100000 * Integer.parseInt(jLim.getText()));
          while (running) pool.execute(new NanoDBWorker(dbSvr.accept(), nanoMgr));
        } catch (Exception e) { }
        if (go) try {
          dbSvr.close();
        } catch (Exception ex) { }
        pool.shutdownNow();
        if (running) {
          JOptionPane.showMessageDialog(this, "Cannot start NanoDBServer. Pls. check "+host+":"+port,
                                        "ERROR", JOptionPane.ERROR_MESSAGE);
          System.exit(0);
        }
      });
    });
    JPanel jptop = new JPanel();
    jptop.add(new JLabel("HostName/IP")); jptop.add(jhost);
    jptop.add(new JLabel("PortNumber")); jptop.add(jport);
    jptop.add(lab);
    
    JPanel jpSouth = new JPanel(new GridLayout(2,1));
    JPanel p0 = new JPanel(), p1 = new JPanel();
    jpSouth.add(p0); jpSouth.add(p1);
    //
    p0.add(new JLabel("NanoDB Cache")); p0.add(jLim); p0.add(new JLabel("Min.1 MB - Max.1024 MB)"));
    p1.add(new JLabel("NanoDB Path")); p1.add(jpath); p1.add(start);
    //
    SysMonSWING sysmon = new SysMonSWING(600, 500);
    pool.execute(sysmon); // start SystemMonitor
    
    add("North", jptop);
    add("Center", sysmon);
    add("South", jpSouth);
    
    // setUndecorated(true);
    getRootPane().setBorder(BorderFactory.createMatteBorder(5, 5, 5, 5, Color.LIGHT_GRAY));
    
    setLocation(0, 0);
    pack();
    setVisible(true);
  }
  //
  private NanoDBManager nanoMgr;
  private volatile boolean running = true;
  private ServerSocketChannel dbSvr = null;
 }
  