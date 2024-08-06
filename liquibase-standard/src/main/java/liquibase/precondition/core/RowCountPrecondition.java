package liquibase.precondition.core;

import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.executor.ExecutorService;
import liquibase.precondition.AbstractPrecondition;
import liquibase.statement.core.TableRowCountStatement;
import liquibase.util.StringUtil;
import lombok.Getter;
import lombok.Setter;

@Getter
public class RowCountPrecondition extends AbstractPrecondition {

    @Setter
    private String catalogName;
    private String schemaName;
    @Setter
    private String tableName;
    @Setter
    private Long expectedRows;

    public void setSchemaName(String schemaName) {
        this.schemaName = StringUtil.trimToNull(schemaName);
    }

    @Override
    public Warnings warn(Database database) {
        return new Warnings();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", tableName);
        validationErrors.checkRequiredField("expectedRows", expectedRows);

        return validationErrors;
    }

    @Override
    public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener)
            throws PreconditionFailedException, PreconditionErrorException {
        try {
            TableRowCountStatement statement = new TableRowCountStatement(catalogName, schemaName, tableName);

            long result = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForLong(statement);
            if (result != expectedRows) {
                throw new PreconditionFailedException(getFailureMessage(result, expectedRows), changeLog, this);
            }

        } catch (PreconditionFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new PreconditionErrorException(e, changeLog, this);
        }
    }

    protected String getFailureMessage(long result, long expectedRows) {
        return "Table "+tableName+" does not have the expected row count of "+expectedRows+". It contains "+result+" rows";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @Override
    public String getName() {
        return "rowCount";
    }

}
