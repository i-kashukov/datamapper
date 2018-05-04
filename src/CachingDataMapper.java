import java.util.Map;

public interface CachingDataMapper<T> {

    Map<Long, String> getAll();
    T getById(long id);
    long persist (T object);
    void delete (T object);
    int size();

}
