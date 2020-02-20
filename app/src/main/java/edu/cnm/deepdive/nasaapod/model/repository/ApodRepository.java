package edu.cnm.deepdive.nasaapod.model.repository;

import android.app.Application;
import android.os.Environment;
import androidx.annotation.NonNull;
import androidx.lifecycle.LiveData;
import edu.cnm.deepdive.nasaapod.BuildConfig;
import edu.cnm.deepdive.nasaapod.model.dao.AccessDao;
import edu.cnm.deepdive.nasaapod.model.dao.ApodDao;
import edu.cnm.deepdive.nasaapod.model.entity.Access;
import edu.cnm.deepdive.nasaapod.model.entity.Apod;
import edu.cnm.deepdive.nasaapod.model.entity.Apod.MediaType;
import edu.cnm.deepdive.nasaapod.model.pojo.ApodWithStats;
import edu.cnm.deepdive.nasaapod.service.ApodDatabase;
import edu.cnm.deepdive.nasaapod.service.ApodService;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.schedulers.Schedulers;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

public class ApodRepository {

  private static final int NETWORK_THREAD_COUNT = 10;

  private final ApodDatabase database;
  private final ApodService nasa;
  private final Executor networkPool;

  private static Application context;
  private static final Pattern URL_FILENAME_PATTERN = Pattern.compile("^.*/([^/#?]+)(?:\\?.*)?(?:#.*)?$");
  public static final String  LOCAL_FILENAME_FORMAT = "%1$tY%1$tm%1$td-%2$s";

  private ApodRepository() {
    if (context == null) {
      throw new IllegalStateException();
    }
    database = ApodDatabase.getInstance();
    nasa = ApodService.getInstance();
    networkPool = Executors.newFixedThreadPool(NETWORK_THREAD_COUNT);
  }

  public static void setContext(Application context) {
    ApodRepository.context = context;
  }

  public static ApodRepository getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public Single<Apod> get(Date date) {
    ApodDao dao = database.getApodDao();
    return dao.select(date)
        .subscribeOn(Schedulers.io())
        .switchIfEmpty((SingleSource<? extends Apod>) (observer) ->
            nasa.get(BuildConfig.API_KEY, ApodService.DATE_FORMATTER.format(date))
                .subscribeOn(Schedulers.from(networkPool))
                .flatMap((apod) ->
                    dao.insert(apod)
                        .map((id) -> {
                          apod.setId(id);
                          return apod;
                        })
                )
                .subscribe(observer)
        )
        .doAfterSuccess(this::insertAccess);
  }

  public LiveData<List<ApodWithStats>> get() {
    return database.getApodDao().selectWithStats();
  }

  public Single<String> getImage(@NonNull Apod apod) {
    // TODO Add local file download & reference.
    // TODO define a maybe task that looks for a local file and downloads it if there isn't one.
    boolean canBeLocal = (apod.getMediaType() == MediaType.IMAGE);
    File file = canBeLocal ? getFile(apod) : null;
    return Single.fromCallable(apod::getUrl);
  }

  // a file Object has no content, it just points to a location
  private File getFile(@Nonnull Apod apod) {
    String url = apod.getUrl();
    File file= null;
    Matcher matcher = URL_FILENAME_PATTERN.matcher(url);
    if (matcher.matches()) {
      String filename = String.format(LOCAL_FILENAME_FORMAT, apod.getDate(), matcher.group(1));
      File directory = context.getExternalFilesDir(Environment.DIRECTORY_PICTURES);
      if (!Environment.MEDIA_MOUNTED.equals(Environment.getExternalStorageState(directory))) {
        directory = context.getFilesDir();
      }
      file = new File(directory, filename);
    }
    return file;
  }

  private void insertAccess(Apod apod) {
    AccessDao accessDao = database.getAccessDao();
    Access access = new Access();
    access.setApodId(apod.getId());
    accessDao.insert(access)
        .subscribeOn(Schedulers.io())
        .subscribe(/* TODO Handle error result */);
  }

  private static class InstanceHolder {

    private static final ApodRepository INSTANCE = new ApodRepository();

  }

}
