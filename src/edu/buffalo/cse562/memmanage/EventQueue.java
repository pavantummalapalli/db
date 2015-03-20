package edu.buffalo.cse562.memmanage;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class EventQueue {
	
	private static Queue<Event> eventQueue = new ConcurrentLinkedQueue<Event>();
	
	public static void dispatch(Event event){
		eventQueue.add(event);
	}
	
	public static Event pollEvent(){
		Event event  = eventQueue.poll();
		if(event !=null)
			eventQueue.clear();
		return event;
	}
}
