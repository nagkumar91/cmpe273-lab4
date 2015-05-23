package edu.sjsu.cmpe.cache.client;
import com.mashape.unirest.http.Unirest;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CacheServiceInterface cache = new DistributedCacheService( "http://localhost", new Integer(3000), new Integer(3));

        System.out.println("Trying to PUT 'a' to key 1 on all servers");
        cache.put(1, "a");

		System.out.println("Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("Trying to PUT 'b' to key 1 on all servers");
        cache.put(1, "b");
        
        System.out.println("Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("Trying to GET data for key 1 from all servers");
        String value = cache.get(1);
        System.out.println("get(2) => " + value);
        System.out.println("Exiting Cache Client...");


        Unirest.shutdown();
    }

}

        System.out.println("STEP 1: Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("STEP 2: PUT 1 => 'b' on all servers");
        cache.put(1, "b");
        System.out.println("STEP 2: Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("STEP 3: GET 1 from all servers");
        String value = cache.get(1);
        System.out.println("STEP 3: get(2) => " + value);
        System.out.println("Exiting Cache Client...");


        Unirest.shutdown();
    }

}