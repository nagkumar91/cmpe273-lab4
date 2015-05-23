package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CacheServiceInterface cache = new DistributedCacheService( "http://localhost", new Integer(3000), new Integer(3));

        System.out.println("put(2 => bar)");
        cache.put(2, "bar");

        String value = cache.get(2);
        System.out.println("get(2) => " + value);
        System.out.println("Exiting Cache Client...");
    }

}