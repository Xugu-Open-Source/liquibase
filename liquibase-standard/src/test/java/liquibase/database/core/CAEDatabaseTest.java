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
 * Tests for {@link CAEDatabase}
 */
public class CAEDatabaseTest extends AbstractJdbcDatabaseTest {

    public CAEDatabaseTest() throws Exception {
        super(new CAEDatabase());
    }

    @Override
    protected String getProductNameString() {
      return "CAEDB SQL Server";
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
        CAEDatabase caeDatabase = (CAEDatabase) getDatabase();
        Assert.assertEquals("NOW(1)", caeDatabase.getCurrentDateTimeFunction(1));
        Assert.assertEquals("NOW(2)", caeDatabase.getCurrentDateTimeFunction(2));
        Assert.assertEquals("NOW(5)", caeDatabase.getCurrentDateTimeFunction(5));
    }

    @Test
    public void generateDatabaseFunctionValue() {
        CAEDatabase caeDatabase = (CAEDatabase) getDatabase();
        assertEquals("NOW()", caeDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP()")));
        assertNull(caeDatabase.generateDatabaseFunctionValue(new DatabaseFunction(null)));
    }

    @Test
    public void generateDatabaseFunctionValueWithPrecision() {
        CAEDatabase caeDatabase = (CAEDatabase) getDatabase();
        assertEquals("NOW(2)", caeDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(2)")));
        assertEquals("NOW(3)", caeDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(3)")));
    }

    @Test
    public void generateDatabaseFunctionValueWithIncorrectPrecision() {
        CAEDatabase caeDatabase = (CAEDatabase) getDatabase();
        assertEquals("NOW()", caeDatabase.generateDatabaseFunctionValue(new DatabaseFunction("CURRENT_TIMESTAMP(string)")));
    }

    public void testGetDefaultDriver() {
        Database database = new CAEDatabase();

        assertEquals("com.cae.cloudjdbc.Driver", database.getDefaultDriver("jdbc:cae://127.0.0.1:5138/liquibase"));

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
