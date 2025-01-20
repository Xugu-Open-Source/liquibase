package liquibase.database.core;

import liquibase.database.AbstractJdbcDatabaseTest;
import liquibase.database.Database;
import liquibase.statement.DatabaseFunction;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link XuguDatabase}
 */
public class XuGuDatabaseTest extends AbstractJdbcDatabaseTest {

    public XuGuDatabaseTest() throws Exception {
        super(new XuguDatabase());
    }

    @Override
    protected String getProductNameString() {
      return "XuguDB";
    }

    @Override
    @Test
    public void supportsInitiallyDeferrableColumns() {
        assertFalse(getDatabase().supportsInitiallyDeferrableColumns());
    }



    @Override
    @Test
    public void getCurrentDateTimeFunction() {
        Assert.assertEquals("NOW()", getDatabase().getCurrentDateTimeFunction());
    }

    @Test
    public void getCurrentDateTimeFunctionWithPrecision() {
        XuguDatabase xuguDatabase = (XuguDatabase) getDatabase();
        Assert.assertEquals("NOW(1)", xuguDatabase.getCurrentDateTimeFunction(1));
        Assert.assertEquals("NOW(2)", xuguDatabase.getCurrentDateTimeFunction(2));
        Assert.assertEquals("NOW(5)", xuguDatabase.getCurrentDateTimeFunction(5));
    }

    @Test
    public void generateDatabaseFunctionValue() {
        XuguDatabase xuguDatabase = (XuguDatabase) getDatabase();
        assertEquals("NOW()", xuguDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP()")));
        assertNull(xuguDatabase.generateDatabaseFunctionValue(new DatabaseFunction(null)));
    }

    @Test
    public void generateDatabaseFunctionValueWithPrecision() {
        XuguDatabase xuguDatabase = (XuguDatabase) getDatabase();
        assertEquals("NOW(2)", xuguDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(2)")));
        assertEquals("NOW(3)", xuguDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(3)")));
    }

    @Test
    public void generateDatabaseFunctionValueWithIncorrectPrecision() {
        XuguDatabase xuguDatabase = (XuguDatabase) getDatabase();
        assertEquals("NOW()", xuguDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(string)")));
    }

    public void testGetDefaultDriver() {
        Database database = new XuguDatabase();

        assertEquals("com.xugu.cloudjdbc.Driver", database.getDefaultDriver("jdbc:xugu://127.0.0.1:5138/liquibase"));

        assertNull(database.getDefaultDriver("jdbc:db2://localhost;databaseName=liquibase"));
    }

    @Override
    @Test
    public void escapeTableName_noSchema() {
        Database database = getDatabase();
        assertEquals("tableName", database.escapeTableName(null, null, "tableName"));
    }

    @Override
    @Test
    public void escapeTableName_withSchema() {
        Database database = getDatabase();
        assertEquals("schemaName.tableName", database.escapeTableName("catalogName", "schemaName", "tableName"));
    }

    @Test
    public void escapeStringForDatabase_withBackslashes() {
        Assert.assertEquals("\\\\0", database.escapeStringForDatabase("\\0"));
    }

}
