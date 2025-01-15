package liquibase.database.core;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Index;
import liquibase.structure.core.PrimaryKey;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XuguDatabase extends AbstractJdbcDatabase {


    public static final String PRODUCT_NAME = "XuguDB";
    private static final Set<String> RESERVED_WORDS = createReservedWords();

    /** Pattern used to extract function precision like 3 in CURRENT_TIMESTAMP(3) */
    private static final String  PRECISION_REGEX = "\\(\\d+\\)";
    public static final Pattern PRECISION_PATTERN = Pattern.compile(PRECISION_REGEX);

    public XuguDatabase() {
        super.setCurrentDateTimeFunction("NOW()");
    }

    @Override
    public String getShortName() {
        return "xugu";
    }

    @Override
    public String correctObjectName(String name, Class<? extends DatabaseObject> objectType) {
        if (objectType.equals(PrimaryKey.class) && "PRIMARY".equals(name)) {
            return null;
        } else {
            name = super.correctObjectName(name, objectType);
            if (name == null) {
                return null;
            }
            return name;
        }
    }
    @Override
    protected String getDefaultDatabaseProductName() {
        return "XuguDB";
    }

    @Override
    public Integer getDefaultPort() {
        return 5138;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        //noinspection HardCodedStringLiteral
        if (url.startsWith("jdbc:xugu")) {
            return "com.xugu.cloudjdbc.Driver";
        }
        return null;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    protected boolean mustQuoteObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        return super.mustQuoteObjectName(objectName, objectType) || (!objectName.contains("(") && !objectName.matches("\\w+"));
    }

    @Override
    public String getLineComment() {
        return "-- ";
    }

    @Override
    protected String getAutoIncrementClause() {
        return "IDENTITY";
    }

    @Override
    public String getAutoIncrementClause(BigInteger startWith, BigInteger incrementBy, String generationType, Boolean defaultOnNull) {
        String clause = getAutoIncrementClause();
        boolean generateStartWith = generateAutoIncrementStartWith(startWith);
        boolean generateIncrementBy = generateAutoIncrementBy(incrementBy);
        if (generateStartWith || generateIncrementBy) {
            return clause.replaceAll("\\(.*?\\)", "") + "(" +
                    ((startWith == null) ? defaultAutoIncrementStartWith : startWith) +
                    "," +
                    ((incrementBy == null) ? defaultAutoIncrementBy : incrementBy) +
                    ")";
        }
        return clause;
    }

    @Override
    protected String getAutoIncrementStartWithClause() {
        return "=%d";
    }

    @Override
    public String getConcatSql(String... values) {
        StringBuilder returnString = new StringBuilder();
        returnString.append("CONCAT_WS(");
        for (String value : values) {
            returnString.append(value).append(", ");
        }

        return returnString.toString().replaceFirst(", $", ")");
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }



    @Override
    public String escapeIndexName(String catalogName, String schemaName, String indexName) {
        return escapeObjectName(indexName, Index.class);
    }

    @Override
    public boolean supportsForeignKeyDisable() {
        return true;
    }

    @Override
    public boolean disableForeignKeyChecks() throws DatabaseException {
        boolean enabled = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).queryForInt(new RawSqlStatement("SELECT @@FOREIGN_KEY_CHECKS")) == 1;
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).execute(new RawSqlStatement("SET FOREIGN_KEY_CHECKS=0"));
        return enabled;
    }

    @Override
    public void enableForeignKeyChecks() throws DatabaseException {
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).execute(new RawSqlStatement("SET FOREIGN_KEY_CHECKS=1"));
    }

    @Override
    public CatalogAndSchema getSchemaFromJdbcInfo(String rawCatalogName, String rawSchemaName) {
        return new CatalogAndSchema(rawCatalogName, null).customize(this);
    }

    @Override
    public String escapeStringForDatabase(String string) {
        string = super.escapeStringForDatabase(string);
        if (string == null) {
            return null;
        }
        return string.replace("\\", "\\\\");
    }

    @Override
    public boolean createsIndexesForForeignKeys() {
        return true;
    }

    @Override
    public boolean isReservedWord(String string) {
        if (RESERVED_WORDS.contains(string.toUpperCase())) {
            return true;
        }
        return super.isReservedWord(string);
    }


    @Override
    protected String getQuotingStartCharacter() {
        return "`"; // objects in mysql are always case sensitive

    }

    @Override
    protected String getQuotingEndCharacter() {
        return "`"; // objects in mysql are always case sensitive
    }

    /**
     * <p>Returns the default timestamp fractional digits if nothing is specified.</p>
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html">fractional seconds</a>:
     * "The fsp value, if given, must be in the range 0 to 6. A value of 0 signifies that there is no fractional part.
     * If omitted, the default precision is 0. (This differs from the STANDARD SQL default of 6, for compatibility
     * with previous MySQL versions.)"
     *
     * @return always 0
     */
    @Override
    public int getDefaultFractionalDigitsForTimestamp() {
        return 0;
    }

    /*
     * list from https://help.xugudb.com/documents/sql-syntax-reference-guide/keywords-L
     */
    private static Set<String> createReservedWords() {
        return new HashSet<>(Arrays.asList("ABORT",
                "ABOVE",
                "ABORT",
                "ABOVE",
                "ABSOLUTE",
                "ACCESS",
                "ACCOUNT",
                "ACTION",
                "ADD",
                "AFTER",
                "AGGREGATE",
                "ALL",
                "ALL_ROWS",
                "ALTER",
                "ANALYSE",
                "ANALYZE",
                "AND",
                "ANY",
                "AOVERLAPS",
                "APPEND",
                "ARCHIVELOG",
                "ARE",
                "ARRAY",
                "AS",
                "ASC",
                "AT",
                "AUDIT",
                "AUDITOR",
                "AUTHID",
                "AUTHORIZATION",
                "AUTO",
                "BACKUP",
                "BACKWARD",
                "BADFILE",
                "BCONTAINS",
                "BEFORE",
                "BEGIN",
                "BETWEEN",
                "BINARY",
                "BINTERSECTS",
                "BIT",
                "BLOCK",
                "BLOCKS",
                "BODY",
                "BOTH",
                "BOUND",
                "BOVERLAPS",
                "BREAK",
                "BUFFER_POOL",
                "BUILD",
                "BULK",
                "BWITHIN",
                "BY",
                "CACHE",
                "CALL",
                "CASCADE",
                "CASE",
                "CAST",
                "CATCH",
                "CATEGORY",
                "CHAIN",
                "CHAR",
                "CHARACTER",
                "CHARACTERISTICS",
                "CHECK",
                "CHECKPOINT",
                "CHOOSE",
                "CHUNK",
                "CLOSE",
                "CLUSTER",
                "COALESCE",
                "COLLATE",
                "COLLECT",
                "COLUMN",
                "COLUMNS",
                "COMMENT",
                "COMMIT",
                "COMMITTED",
                "COMPLETE",
                "COMPRESS",
                "COMPUTE",
                "CONNECT",
                "CONNECT_NODES",
                "CONSTANT",
                "CONSTRAINT",
                "CONSTRAINTS",
                "CONSTRUCTOR",
                "CONTAINS",
                "CONTEXT",
                "CONTINUE",
                "COPY",
                "CORRESPONDING",
                "CPU_PER_CALL",
                "CPU_PER_SESSION",
                "CREATE",
                "CREATEDB",
                "CREATEUSER",
                "CROSS",
                "CROSSES",
                "CUBE",
                "CURRENT",
                "CURSOR",
                "CURSOR_QUOTA",
                "CYCLE",
                "DATABASE",
                "DATAFILE",
                "DATE",
                "DATETIME",
                "DAY",
                "DBA",
                "DEALLOCATE",
                "DEC",
                "DECIMAL",
                "DECLARE",
                "DECODE",
                "DECRYPT",
                "DEFAULT",
                "DEFERRABLE",
                "DEFERRED",
                "DELETE",
                "DELIMITED",
                "DELIMITERS",
                "DEMAND",
                "DESC",
                "DESCRIBE",
                "DETERMINISTIC",
                "DIR",
                "DISABLE",
                "DISASSEMBLE",
                "DISCORDFILE",
                "DISJOINT",
                "DISTINCT",
                "DO",
                "DOMAIN",
                "DOUBLE",
                "DRIVEN",
                "DROP",
                "EACH",
                "ELEMENT",
                "ELSE",
                "ELSEIF",
                "ELSIF",
                "ENABLE",
                "ENCODING",
                "ENCRYPT",
                "ENCRYPTOR",
                "END",
                "ENDCASE",
                "ENDFOR",
                "ENDIF",
                "ENDLOOP",
                "EQUALS",
                "ESCAPE",
                "EVERY",
                "EXCEPT",
                "EXCEPTION",
                "EXCEPTIONS",
                "EXCEPTION_INIT",
                "EXCLUSIVE",
                "EXEC",
                "EXECUTE",
                "EXISTS",
                "EXIT",
                "EXPIRE",
                "EXPLAIN",
                "EXPORT",
                "EXTEND",
                "EXTERNAL",
                "EXTRACT",
                "FAILED_LOGIN_ATTEMPTS",
                "FALSE",
                "FAST",
                "FETCH",
                "FIELD",
                "FIELDS",
                "FILTER",
                "FINAL",
                "FINALLY",
                "FIRST",
                "FIRST_ROWS",
                "FLASHBACK",
                "FLOAT",
                "FOLLOWING",
                "FOR",
                "FORALL",
                "FORCE",
                "FOREIGN",
                "FORWARD",
                "FOUND",
                "FREELIST",
                "FREELISTS",
                "FROM",
                "FULL",
                "FUNCTION",
                "G",
                "GENERATED",
                "GET",
                "GLOBAL",
                "GOTO",
                "GRANT",
                "GREATEST",
                "GROUP",
                "GROUPING",
                "GROUPS",
                "HANDLER",
                "HASH",
                "HAVING",
                "HEAP",
                "HIDE",
                "HINT",
                "HOTSPOT",
                "HOUR",
                "IDENTIFIED",
                "IDENTIFIER",
                "IDENTITY",
                "IF",
                "IGNORE",
                "ILIKE",
                "IMMEDIATE",
                "IMPORT",
                "IN",
                "GROUPS",
                "INCLUDE",
                "INCREMENT",
                "INDEX",
                "INDEXTYPE",
                "INDEX_ASC",
                "INDEX_DESC",
                "INDEX_FSS",
                "INDEX_JOIN",
                "INDICATOR",
                "INDICES",
                "INHERITS",
                "INIT",
                "INITIAL",
                "INITIALLY",
                "INITRANS",
                "INNER",
                "INOUT",
                "INSENSITIVE",
                "INSERT",
                "INSTANTIABLE",
                "INSTEAD",
                "INTERSECT",
                "INTERSECTS",
                "INTERVAL",
                "INTO",
                "IO",
                "IS",
                "ISNULL",
                "ISOLATION",
                "ISOPEN",
                "JOB",
                "JOIN",
                "K",
                "KEEP",
                "KEY",
                "KEYSET",
                "LABEL",
                "LANGUAGE",
                "LAST",
                "LEADING",
                "LEAST",
                "LEAVE",
                "LEFT",
                "LEFTOF",
                "LENGTH",
                "LESS",
                "LEVEL",
                "LEVELS",
                "LEXER",
                "LIBRARY",
                "LIKE",
                "LIMIT",
                "LINK",
                "LIST",
                "LISTEN",
                "LOAD",
                "LOB",
                "LOCAL",
                "LOCATION",
                "LOCATOR",
                "LOCK",
                "LOGFILE",
                "LOGGING",
                "LOGIN",
                "LOGOFF",
                "LOGON",
                "LOGOUT",
                "LOOP",
                "LOVERLAPS",
                "M",
                "MATCH",
                "MATCHED",
                "MATERIALIZED",
                "MAX",
                "MAXEXTENTS",
                "MAXSIZE",
                "MAXTRANS",
                "MAXVALUE",
                "MAXVALUES",
                "MAX_CONNECT_TIME",
                "MAX_IDLE_TIME",
                "MAX_STORE_NUM",
                "MEMBER",
                "MEMORY",
                "MERGE",
                "MINEXTENTS",
                "MINUS",
                "MINUTE",
                "MINVALUE",
                "MISSING",
                "MODE",
                "MODIFY",
                "MONTH",
                "MOVEMENT",
                "NAME",
                "NAMES",
                "NATIONAL",
                "NATURAL",
                "NCHAR",
                "NESTED",
                "NEW",
                "NEWLINE",
                "NEXT",
                "NO",
                "NOAPPEND",
                "NOARCHIVELOG",
                "NOAUDIT",
                "NOCACHE",
                "NOCOMPRESS",
                "NOCREATEDB",
                "NOCREATEUSER",
                "NOCYCLE",
                "NODE",
                "NOFORCE",
                "NOFOUND",
                "NOINDEX",
                "NOLOGGING",
                "NONE",
                "NOORDER",
                "NOPARALLEL",
                "NOT",
                "NOTFOUND",
                "NOTHING",
                "NOTIFY",
                "NOTNULL",
                "NOVALIDATE",
                "NOWAIT",
                "NULL",
                "NULLIF",
                "NULLS",
                "NUMBER",
                "NUMERIC",
                "NVARCHAR",
                "NVARCHAR2",
                "NVL",
                "NVL2",
                "OBJECT",
                "OF",
                "OFF",
                "OFFLINE",
                "OFFSET",
                "OIDINDEX",
                "OIDS",
                "OLD",
                "ON",
                "ONLINE",
                "ONLY",
                "OPEN",
                "OPERATOR",
                "OPTION",
                "OR",
                "ORDER",
                "ORDERD",
                "ORGANIZATION",
                "OTHERVALUES",
                "OUT",
                "OUTER",
                "OVER",
                "OVERLAPS",
                "OWNER",
                "PACKAGE",
                "PARALLEL",
                "PARAMETERS",
                "PARTIAL",
                "PARTITION",
                "PARTITIONS",
                "PASSWORD",
                "PASSWORD_LIFE_PERIOD",
                "PASSWORD_LOCK_TIME",
                "PCTFREE",
                "PCTINCREASE",
                "PCTUSED",
                "PCTVERSION",
                "PERIOD",
                "POLICY",
                "PRAGMA",
                "PREBUILT",
                "PRECEDING",
                "PRECISION",
                "PREPARE",
                "PRESERVE",
                "PRIMARY",
                "PRIOR",
                "PRIORITY",
                "PRIVATE_SGA",
                "PRIVILEGES",
                "PROCEDURAL",
                "PROCEDURE",
                "PROFILE",
                "PROTECTED",
                "PUBLIC",
                "QUERY",
                "QUOTA",
                "RAISE",
                "RANGE",
                "RAW",
                "READ",
                "READS",
                "READS_PER_CALL",
                "READS_PER_SESSION",
                "REBUILD",
                "RECOMPILE",
                "RECORD",
                "RECORDS",
                "RECYCLE",
                "REDUCED",
                "REF",
                "REFERENCES",
                "REFERENCING",
                "REFRESH",
                "REINDEX",
                "RELATIVE",
                "RENAME",
                "REPEATABLE",
                "REPLACE",
                "REPLICATION",
                "RESOURCE",
                "RESTART",
                "RESTORE",
                "RESTRICT",
                "RESULT",
                "RESULT_CACHE",
                "PROTECTED",
                "RETURN",
                "RETURNING",
                "REVERSE",
                "REVOKE",
                "REWRITE",
                "RIGHT",
                "RIGHTOF",
                "ROLE",
                "ROLLBACK",
                "ROLLUP",
                "ROVERLAPS",
                "ROW",
                "ROWCOUNT",
                "ROWID",
                "ROWS",
                "ROWTYPE",
                "RULE",
                "RUN",
                "SAVEPOINT",
                "SCHEMA",
                "SCROLL",
                "SECOND",
                "SEGMENT",
                "SELECT",
                "SELF",
                "SEQUENCE",
                "SERIALIZABLE",
                "SESSION",
                "SESSION_PER_USER",
                "SET",
                "SETOF",
                "SETS",
                "SHARE",
                "SHOW",
                "SHUTDOWN",
                "SIBLINGS",
                "SIZE",
                "SLOW",
                "SNAPSHOT",
                "SOME",
                "SPATIAL",
                "SPLIT",
                "SSO",
                "STANDBY",
                "START",
                "STATEMENT",
                "STATIC",
                "STATISTICS",
                "STEP",
                "STOP",
                "STORAGE",
                "STORE",
                "STORE_NODES",
                "STREAM",
                "SUBPARTITION",
                "SUBPARTITIONS",
                "SUBTYPE",
                "SUCCESSFUL",
                "SYNONYM",
                "SYSTEM",
                "TABLE",
                "TABLESPACE",
                "TEMP",
                "TEMPLATE",
                "TEMPORARY",
                "TEMPSPACE_QUOTA",
                "TERMINATED",
                "THAN",
                "THEN",
                "THROW",
                "TIME",
                "TIMESTAMP",
                "TO",
                "TOP",
                "TOPOVERLAPS",
                "TOTAL_RESOURCE_LIMIT",
                "TOUCHES",
                "TRACE",
                "TRAILING",
                "TRAN",
                "TRANSACTION",
                "TRIGGER",
                "TRUE",
                "TRUNCATE",
                "TRUSTED",
                "TRY",
                "TYPE",
                "UNBOUNDED",
                "UNDER",
                "UNDO",
                "UNIFORM",
                "UNION",
                "UNIQUE",
                "UNLIMITED",
                "UNLISTEN",
                "UNLOCK",
                "UNPROTECTED",
                "UNTIL",
                "UOVERLAPS",
                "UPDATE",
                "USE",
                "USER",
                "USE_HASH",
                "USING",
                "VACUUM",
                "VALID",
                "VALIDATE",
                "VALUE",
                "VALUES",
                "VARCHAR",
                "VARCHAR2",
                "VARRAY",
                "VARYING",
                "VERBOSE",
                "VERSION",
                "VIEW",
                "VOCABLE",
                "WAIT",
                "WHEN",
                "WHENEVER",
                "WHERE",
                "WHILE",
                "WITH",
                "WITHIN",
                "WITHOUT",
                "WORK",
                "WRITE",
                "XML",
                "YEAR",
                "ZONE"
        ));
    }

    protected String getCurrentDateTimeFunction(int precision) {
        return currentDateTimeFunction.replace("()", "("+precision+")");
    }

    @Override
    public String generateDatabaseFunctionValue(DatabaseFunction databaseFunction) {
        if (databaseFunction.getValue() != null && isCurrentTimeFunction(databaseFunction.getValue().toLowerCase())) {
            if (databaseFunction.getValue().toLowerCase().contains("on update")) {
                return databaseFunction.getValue();
            }
            int precision = extractPrecision(databaseFunction);
            return precision != 0 ? getCurrentDateTimeFunction(precision) : getCurrentDateTimeFunction();
        }
        return super.generateDatabaseFunctionValue(databaseFunction);
    }

    private int extractPrecision(DatabaseFunction databaseFunction) {
        int precision = 0;
        Matcher precisionMatcher = PRECISION_PATTERN.matcher(databaseFunction.getValue());
        if (precisionMatcher.find()) {
            precision = Integer.parseInt(precisionMatcher.group().replaceAll("[(,)]", ""));
        }
        return precision;
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawCallStatement("select current_schema()");
    }
}
