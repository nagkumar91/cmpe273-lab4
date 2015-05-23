package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;


/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String caching_server_URL;
    private final Integer start_port;
    private final Integer number_of_service;
    private final int write_Q;
    private final int read_Q;
    private AtomicInteger number_of_success;
    private AtomicInteger number_of_attempts;
    private String[] get_values;
    private AtomicInteger get_index;
    private CountDownLatch response_waiter;



    public DistributedCacheService(String server_URL, Integer start_port, Integer number_of_service) {
        this.write_Q = this.read_Q = 2;
        this.number_of_success = new AtomicInteger(0);
        this.number_of_attempts = new AtomicInteger(0);
        this.number_of_service = number_of_service;
        this.caching_server_URL = server_URL;
        this.start_port = start_port;
        this.get_values = new String[this.number_of_service];
        this.get_index = new AtomicInteger(0);
        this.response_waiter = new CountDownLatch(this.number_of_service);
    }

    // naive method to return string with maximum occurrence
    private String getread_QString() {

        HashMap<String, Integer> cntMap = new HashMap<String, Integer>();
        int maxCount = 1;
        String  retVal = this.get_values[0];
        for (int j = 0; j < this.get_index.get(); j++) {
            if (cntMap.containsKey(this.get_values[j])) {
                int count = cntMap.get(this.get_values[j]).intValue();
                cntMap.put(this.get_values[j], new Integer(count + 1));
                if (count + 1 > maxCount) {
                    retVal = this.get_values[j];
                    maxCount = count + 1;
                }
            } else {
                cntMap.put(this.get_values[j], 1);
            }
        }
        if (maxCount == this.number_of_service) {
            
            retVal = null;
        } else if ((maxCount < this.read_Q) && (retVal != null)) {
            System.out.println("read Quorum not met.");
            
            for (int j = 0; j < this.get_index.get(); j++) {
            
                this.get_values[j] = null;
            }
        }
        return retVal;
    }
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        String value = null;
        int i = 0;
        while (i < this.number_of_service.intValue()) {
            Integer port = this.start_port + new Integer(i);

            String cacheUrl = this.caching_server_URL + ":" + port.toString();
            System.out.println("Getting from server " + cacheUrl);
            Future<HttpResponse<JsonNode>> future = Unirest.get(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.number_of_attempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Failed to get data from the cache. Have to perform a repair");
                                } else {
                                    
                                    DistributedCacheService.this.number_of_success.getAndIncrement();
                                    DistributedCacheService.this.get_values[DistributedCacheService.this.get_index.get()] =
                                            httpResponse.getBody().getObject().getString("value");
                                    //System.out.println(DistributedCacheService.this.get_index.get() + " => " + httpResponse.getBody().getObject().getString("value"));
                                    DistributedCacheService.this.get_index.getAndIncrement();
                                }
                                response_waiter.countDown();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.number_of_attempts.getAndIncrement();
                            System.out.println("Failed to GET data. May need repair");
                            response_waiter.countDown();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.number_of_attempts.getAndIncrement();
                            System.out.println("Cancelled");
                            response_waiter.countDown();
                        }
                    });

            i++;
        }
        try {
            this.response_waiter.await();
        } catch (Exception e) {
            System.out.println("Error with exception " + e);
        }

        String repairString = getread_QString();
        if (repairString != null) {
            System.out.println("String to repair with: " + repairString);
            put(key, repairString);
        } else {
            repairString = this.get_values[0];
        }
        return repairString;
    }

    @Override
    public void delete(final long key) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.number_of_service.intValue(); i++) {
            Integer port = this.start_port + new Integer(i);

            String cacheUrl = this.caching_server_URL + ":" + port.toString();
            System.out.println("Deleting from " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .delete(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            if (httpResponse.getCode() != 204) {
                                System.out.println("Failed to del from the cache.");
                            } else {
                                //System.out.println("Deleted " + key);
                                DistributedCacheService.this.number_of_success.getAndIncrement();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            System.out.println("Failed : " + e);
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("Cancelled");
                        }
                    });

        }
        DistributedCacheService.this.number_of_success.set(0);
        DistributedCacheService.this.number_of_attempts.set(0);
    }
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(final long key, final String value) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.number_of_service.intValue(); i++) {
            Integer port = this.start_port + new Integer(i);

            String cacheUrl = this.caching_server_URL + ":" + port.toString();
            System.out.println("Putting to " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .put(cacheUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJsonAsync(new Callback<JsonNode>() {
                        private void checkForSuccess() {
                            int flag = 0;
                            
                            if (DistributedCacheService.this.number_of_attempts.get() == DistributedCacheService.this.number_of_service) {
                                if (DistributedCacheService.this.number_of_success.get() >= DistributedCacheService.this.write_Q) {
                                    
                                } else {
                                    flag = 1;
                                    
                                }

                                DistributedCacheService.this.number_of_attempts.set(0);
                                DistributedCacheService.this.number_of_success.set(0);
                                if (flag == 1) {
                                    DistributedCacheService.this.delete(key);
                                }
                            }
                        }

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.number_of_attempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Could not add to cache");
                                } else {
                                    DistributedCacheService.this.number_of_success.getAndIncrement();
                                }
                                checkForSuccess();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.number_of_attempts.getAndIncrement();
                            System.out.println("Failed to PUT data.");
                            checkForSuccess();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.number_of_attempts.getAndIncrement();
                            System.out.println("Cancelled");
                            checkForSuccess();
                        }
                    });

        }
    }
}