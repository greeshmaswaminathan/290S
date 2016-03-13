package com.systems.s290.data;

public interface IConsistentHashStrategy {
	
	public void removeBin(String serverToRemove);
	public void addBin(String serverToAdd);
	public int getHashForServer(String server);
	public String getBinFor(long userId);

}
