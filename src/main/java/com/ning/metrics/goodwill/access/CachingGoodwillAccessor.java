/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.goodwill.access;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CachingGoodwillAccessor extends Accessor
{
    private static final int DEFAULT_CACHE_TIMEOUT_IN_SECONDS = 90;
    private final Object cacheMonitor = new Object();

    private final ConcurrentHashMap<String, GoodwillSchema> knownSchemata = new ConcurrentHashMap<String, GoodwillSchema>();
    private final GoodwillAccessor delegate;

    public CachingGoodwillAccessor(String host, int port)
    {
        this(host, port, DEFAULT_CACHE_TIMEOUT_IN_SECONDS);
    }

    public CachingGoodwillAccessor(String host, int port, int cacheTimeoutInSeconds)
    {
        super(host, port);

        delegate = new GoodwillAccessor(host, port);

        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                refreshSchemataCache();
            }
        }, 0, cacheTimeoutInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Invalidate and refresh the cache
     * <p/>
     * This is blocking and returns when the cache has been updated
     */
    public void refreshSchemataCache()
    {
        try {
            List<GoodwillSchema> schemata = delegate.getSchemata().get();

            // If Goodwill is down - keep the old cache around
            if (schemata == null) {
                return;
            }

            ConcurrentHashMap<String, GoodwillSchema> newSchemataCache = new ConcurrentHashMap<String, GoodwillSchema>();
            for (GoodwillSchema schema : schemata) {
                newSchemataCache.put(schema.getName(), schema);
            }

            synchronized (cacheMonitor) {
                knownSchemata.clear();
                knownSchemata.putAll(newSchemataCache);
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while refreshing the cache");
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            log.warn(String.format("Unable to refresh schemata cache: %s", e.getLocalizedMessage()));
        }
    }

    /**
     * Given a schema name, get the associated GoodwillSchema. This method tries hard to find it, i.e. it will refresh the
     * cache if the schema is not in the cache.
     *
     * @param schemaName name of the schema to find
     * @return GoodwillSchema describing the schema
     */
    public GoodwillSchema getSchema(String schemaName)
    {
        GoodwillSchema schema = knownSchemata.get(schemaName);
        if (schema == null) {
            refreshSchemataCache();
            schema = knownSchemata.get(schemaName);
        }

        return schema;
    }

    public List<GoodwillSchema> getSchemata()
    {
        return Collections.list(knownSchemata.elements());
    }

    /**
     * Close the underlying http client
     */
    public synchronized void close()
    {
        delegate.close();
    }
}
