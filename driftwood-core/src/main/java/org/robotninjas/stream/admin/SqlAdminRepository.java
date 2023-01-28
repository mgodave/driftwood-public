/**
 * Copyright [2023] David J. Rusek <dave.rusek@gmail.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.robotninjas.stream.admin;

import com.google.common.util.concurrent.AbstractService;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.SubscriberName;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class SqlAdminRepository extends AbstractService implements AdminRepository {
  private final ConnectionSource connectionSource;
  private final Scheduler scheduler;
  private Dao<PersistedStreamInfo, StreamName> streamInfos;
  private Dao<PersistedSubscriberInfo, SubscriberName> subscriberInfos;

  public SqlAdminRepository(Scheduler scheduler, ConnectionSource connectionSource) {
    this.scheduler = scheduler;
    this.connectionSource = connectionSource;
  }

  @Override
  protected void doStart() {
    try {
      TableUtils.createTableIfNotExists(connectionSource, PersistedStreamInfo.class);
      TableUtils.createTableIfNotExists(connectionSource, PersistedSubscriberInfo.class);
      this.streamInfos = DaoManager.createDao(connectionSource, PersistedStreamInfo.class);
      this.subscriberInfos = DaoManager.createDao(connectionSource, PersistedSubscriberInfo.class);
      notifyStarted();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      scheduler.dispose();
      connectionSource.close();
      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  @Override
  public Mono<StreamInfo> getStream(StreamName stream) {
    return Mono.fromCallable(() ->
      (StreamInfo) streamInfos.queryForId(stream)
    ).subscribeOn(scheduler);
  }

  @Override
  public Mono<SubscriberInfo> getSubscriber(SubscriberName subscriber) {
    return Mono.fromCallable(() ->
      (SubscriberInfo) subscriberInfos.queryForId(subscriber)
    ).subscribeOn(scheduler);
  }

}
