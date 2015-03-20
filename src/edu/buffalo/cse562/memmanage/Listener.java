package edu.buffalo.cse562.memmanage;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.Collection;

import javax.management.Notification;
import javax.management.NotificationListener;

public class Listener implements NotificationListener {

	public Listener() {
	}

	@Override
	public void handleNotification(Notification notification, Object handback) {
		if(notification.getType()==MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED){
			Collection<MemoryPoolMXBean> pool = ManagementFactory.getMemoryPoolMXBeans();
			for(MemoryPoolMXBean temp:pool){
				if(temp.getType().compareTo(MemoryType.HEAP)==0){
					System.out.println(temp.getUsage().getUsed());
					//System.out.println(temp.getName()+"	"+temp.getUsage().getUsed()/(1024*1024));
				}
			}
			//EventQueue.dispatch(Event.MEM_THRES_EXCEEDED);
			System.gc();
			System.out.println("GC triggered");
		}
	}
}
