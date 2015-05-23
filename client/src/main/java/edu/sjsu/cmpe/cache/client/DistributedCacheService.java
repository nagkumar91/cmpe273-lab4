package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String caching_server_URL;
    private final String get_URL;
    private final Integer start_port;
    private final Integer number_of_service;
    private final int write_Q;
    private AtomicInteger number_of_success;
    private AtomicInteger number_of_write_attempts;

    public DistributedCacheService(String server_URL, Integer start_port, Integer number_of_service) {
        this.write_Q = 2;
        this.number_of_success = new AtomicInteger(0);
        this.number_of_write_attempts = new AtomicInteger(0);
        this.number_of_service = number_of_service;
        this.caching_server_URL = server_URL;
        this.start_port = start_port;
        this.get_URL = this.caching_server_URL + ":" + this.start_port.toString();

    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        HttpResponse<JsonNode> response = null;
        String value = null;
        for (int i = 0 ; i < this.number_of_service.intValue(); i++) {
            Integer port = this.start_port + new Integer(i);

            String cacheUrl = this.caching_server_URL + ":" + port.toString();
            try {
                response = Unirest.get(cacheUrl + "/cache/{key}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key)).asJson();
                if (response != null) {
                    value = response.getBody().getObject().getString("value");
                    break;
                }
            } catch (UnirestException e) {
                System.err.println(e);
                continue;
            }
        }

        return value;
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
                                System.out.println("Failed to delete from the cache.");
                            } else {
                                System.out.println("Deleted the key: " + key);
                                DistributedCacheService.this.number_of_success.getAndIncrement();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            System.out.println("Failed with exception: " + e);
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("Cancelled");
                        }
                    });

        }
        DistributedCacheService.this.number_of_success.set(0);
        DistributedCacheService.this.number_of_write_attempts.set(0);
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
                            System.out.println(DistributedCacheService.this.number_of_write_attempts.get() + " of " + DistributedCacheService.this.number_of_service);
                            if (DistributedCacheService.this.number_of_write_attempts.get() == DistributedCacheService.this.number_of_service) {
                                if (DistributedCacheService.this.number_of_success.get() >= DistributedCacheService.this.write_Q) {
                                    System.out.println("Success in PUT!");
                                } else {
                                    flag = 1;
                                    System.out.println("Failure in PUT. Rolling back " + DistributedCacheService.this.number_of_success.get() + " " + DistributedCacheService.this.write_Q);
                                }

                                DistributedCacheService.this.number_of_write_attempts.set(0);
                                DistributedCacheService.this.number_of_success.set(0);
                                if (flag == 1) {
                                    DistributedCacheService.this.delete(key);
                                }
                            }
                        }

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            DistributedCacheService.this.number_of_write_attempts.getAndIncrement();
                            if (httpResponse.getCode() != 200) {
                                System.out.println("Could not add to cache");
                            }
                            else {
                                System.out.println("key - " + key + " has value => " + value);
                                DistributedCacheService.this.number_of_success.getAndIncrement();
                            }
                            checkForSuccess();
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.number_of_write_attempts.getAndIncrement();
                            System.out.println("Failed with exception: " + e);
                            checkForSuccess();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.number_of_write_attempts.getAndIncrement();
                            System.out.println("Cancelled");
                            checkForSuccess();
                        }
                    });

        }
    }
}