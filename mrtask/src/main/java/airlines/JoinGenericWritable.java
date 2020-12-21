package airlines;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class JoinGenericWritable extends GenericWritable {

    private Class<? extends Writable>[] CLASSES = null;

    {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                Airline.class,
                Flight.class
        };
    }

    public JoinGenericWritable() {}

    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
