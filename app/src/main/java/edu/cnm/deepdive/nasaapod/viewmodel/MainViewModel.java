package edu.cnm.deepdive.nasaapod.viewmodel;

import android.app.Application;
import androidx.annotation.NonNull;
import androidx.lifecycle.AndroidViewModel;
import androidx.lifecycle.Lifecycle.Event;
import androidx.lifecycle.LifecycleObserver;
import androidx.lifecycle.LiveData;
import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.OnLifecycleEvent;
import edu.cnm.deepdive.nasaapod.model.entity.Apod;
import edu.cnm.deepdive.nasaapod.model.pojo.ApodWithStats;
import edu.cnm.deepdive.nasaapod.model.repository.ApodRepository;
import edu.cnm.deepdive.nasaapod.service.ApodService;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

public class MainViewModel extends AndroidViewModel implements LifecycleObserver {

  private final MutableLiveData<Apod> apod;
  private final MutableLiveData<Throwable> throwable;
  private final MutableLiveData<Set<String>> permissions;
  private final CompositeDisposable pending;
  private final ApodRepository repository;

  public MainViewModel(@NonNull Application application) {
    super(application);
    repository = ApodRepository.getInstance();
    apod = new MutableLiveData<>();
    throwable = new MutableLiveData<>();
    permissions = new MutableLiveData<>(new HashSet<>());
    pending = new CompositeDisposable();
    Date today = new Date();
    String formattedDate = ApodService.DATE_FORMATTER.format(today);
    try {
      setApodDate(ApodService.DATE_FORMATTER
          .parse(formattedDate)); // TODO Investigate adjustment for NASA APOD-relevant time zone.
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public LiveData<List<ApodWithStats>> getAllApodSummaries() {
    return repository.get();
  }

  public LiveData<Apod> getApod() {
    return apod;
  }

  public LiveData<Throwable> getThrowable() {
    return throwable;
  }

  public LiveData<Set<String>> getPermissions() {
    return permissions;
  }

  public void grantPermission(String permission) {
    Set<String> permissions = this.permissions.getValue();
    if (permissions.add(permission)) {
      this.permissions.setValue(permissions);
    }
  }

  public void revokePermission(String permission) {
    Set<String> permissions = this.permissions.getValue();
    if (permissions.remove(permission)) {
      this.permissions.setValue(permissions);
    }
  }

  public void setApodDate(Date date) {
    throwable.setValue(null);
    pending.add(
        repository.get(date)
            .subscribe(
                apod::postValue,
                throwable::postValue
            )
    );
  }

  public void getImage(@NonNull Apod apod, @NonNull Consumer<String> pathConsumer) {
    throwable.setValue(null);
    pending.add(
        repository.getImage(apod)
            // Consume result on main thread, since pathConsumer will probably "touch" the UI.
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                pathConsumer,
                throwable::setValue
            )
    );
  }

  public void downloadImage(@Nonnull Apod apod, Action onSuccess) {
    throwable.setValue(null);
    pending.add(
        repository.downloadImage(apod)
        .observeOn(AndroidSchedulers.mainThread())
        .subscribe(
            onSuccess,
            throwable::setValue
        )
    );
  }

  @SuppressWarnings("unused")
  @OnLifecycleEvent(Event.ON_STOP)
  private void disposePending() {
    pending.clear();
  }

}
