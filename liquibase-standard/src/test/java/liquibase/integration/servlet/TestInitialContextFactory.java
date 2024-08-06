package liquibase.integration.servlet;

import lombok.Setter;

import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.spi.InitialContextFactory;

public class TestInitialContextFactory implements InitialContextFactory {

    private static String originalFactory;

    @Setter
    private static Context initialContext;

    public static void install() {
        originalFactory = System.getProperty(INITIAL_CONTEXT_FACTORY);
        System.setProperty(INITIAL_CONTEXT_FACTORY,
                           TestInitialContextFactory.class.getName());
    }

    public static void uninstall() {
        if (originalFactory == null) {
            System.getProperties().remove(INITIAL_CONTEXT_FACTORY);
        } else {
            System.setProperty(INITIAL_CONTEXT_FACTORY, originalFactory);
        }
    }

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) {
        return initialContext;
    }

}
