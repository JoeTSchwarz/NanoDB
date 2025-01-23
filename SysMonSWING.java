package nanodb;

import java.awt.*;
import java.util.*;
import javax.swing.*;
import java.lang.management.*;
/**
 System Moninor JPanel
 @author Joe T. Schwarz (C)
*/
public class SysMonSWING extends JPanel implements Runnable {
  /**
  Constructor
  @param width int, SysMonSWING Panel width
  @param height int, SysMonSWING Panel height
  */
  public SysMonSWING(int width, int height) {
    super();
    this.width = width;
    this.height = height;
    setPreferredSize(new Dimension(width, height));
    // create the lists
    ax  = new ArrayList<Integer>(20);
    mem = new ArrayList<Integer>(20);
    pTi = new ArrayList<Integer>(20);
    pLo = new ArrayList<Integer>(20);
    cpu = new ArrayList<Integer>(20);
    swp = new ArrayList<Integer>(20);
    vir = new ArrayList<Integer>(20);
  }
  //
  public void run() {
    try {
      com.sun.management.OperatingSystemMXBean osMaxBean = 
          (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      long mem0, mem1, pTi0, pTi1, swp0, swp1, vir0, vir1;
      int x0 = 0, fl = height - 235, fs = height - 290, ft = height - 290;
      int fv = height - 100, fm = height - 145, fc = height - 185;

      pTi0 = osMaxBean.getProcessCpuTime();
      swp0 = osMaxBean.getFreeSwapSpaceSize();
      mem0 = osMaxBean.getFreePhysicalMemorySize();
      vir0 = osMaxBean.getCommittedVirtualMemorySize();
      msg  = "150 mSec. Period, "+osMaxBean.getAvailableProcessors()+
             " cores, Total Mem. "+(osMaxBean.getTotalPhysicalMemorySize()/1048576)+
             " GB, Horizontal Scale 2, Mem. Unit 1024 B";
      
      while (true) {
        x0 += 2; // scaling step 
        vir1 = osMaxBean.getCommittedVirtualMemorySize();
        mem1 = osMaxBean.getFreePhysicalMemorySize();
        swp1 = osMaxBean.getFreeSwapSpaceSize();
        pTi1 = osMaxBean.getProcessCpuTime();

        ax.add(x0);
        swp.add(fs-(int)((swp0 - swp1)/1048576));
        mem.add(fm-(int)((mem1 - mem0)/1048576));
        vir.add(fv-(int)((vir1 - vir0)/1048576));
        pTi.add(ft-(int)((pTi1 - pTi0)/1000000));
        cpu.add(fc-(int)(osMaxBean.getSystemCpuLoad()*100));
        pLo.add(fl-(int)(osMaxBean.getProcessCpuLoad()*100));

        repaint();
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(150);
        if (x0 >= width) {
          ax.clear();
          mem.clear();
          pTi.clear();
          pLo.clear();
          cpu.clear();
          swp.clear();
          vir.clear();
          x0 = 0;
        }
        mem0 = mem1;
        swp0 = swp1;
        pTi0 = pTi1;
        vir0 = vir1;
        System.gc();
      }
    } catch (Exception e) { }
  }
  //
  protected void paintComponent(Graphics g) {
    g.setColor(Color.black);
    g.fillRect(0, 0, width, height);
    g.setColor(Color.white);
    g.setFont(new Font("veranda", Font.BOLD,12));
    g.drawString("NanoDB SERVER ACTIVITIES MONITORING", 150, 15);

    int h = height-40;
    g.setColor(Color.cyan);
    g.drawLine(10, h, 30, h);
    g.drawString("SwapSpace", 35, h);
    g.setColor(Color.green);
    g.drawLine(120, h, 140, h);
    g.drawString("CPU-Load", 145, h);
    g.setColor(Color.blue);
    g.drawLine(230, h, 250, h);
    g.drawString("Virtual Mem.", 255, h);

    h = height-25;
    g.setColor(Color.pink);
    g.drawLine(230, h, 250, h);
    g.drawString("ProcessTime", 255, h);
    g.setColor(Color.red);
    g.drawLine(120, h, 140, h);
    g.drawString("ProcessLoad", 145, h);
    g.setColor(Color.yellow);
    g.drawLine(10, h, 30, h);
    g.drawString("Used Memory", 35, h);
    g.setColor(Color.white);
    g.drawString(msg, 10, height-10);
    for (int b, e, i = 0, j = 1, mx = pLo.size(); j < mx; ++i, ++j) {
      b = ax.get(i);
      e = ax.get(j);
      g.setColor(Color.yellow);
      g.drawLine(b, mem.get(i), e, mem.get(j));
      g.setColor(Color.red);
      g.drawLine(b, pLo.get(i), e, pLo.get(j));
      g.setColor(Color.pink);
      g.drawLine(b, pTi.get(i), e, pTi.get(j));
      g.setColor(Color.green);
      g.drawLine(b, cpu.get(i), e, cpu.get(j));
      g.setColor(Color.cyan);
      g.drawLine(b, swp.get(i), e, swp.get(j));
      g.setColor(Color.blue);
      g.drawLine(b, vir.get(i), e, vir.get(j));
    }
  }
  //
  private String msg = "";
  private int width, height;
  private ArrayList<Integer> ax, mem, pLo, pTi, cpu, swp, vir;
}
