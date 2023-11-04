package main

import (
	"fmt"
	"strings"

	pb "greateapot.re/dblabs-api"
)

func failBuildQuery(format string, a ...any) (query string, err error) {
	return "", fmt.Errorf("error while building query: %s", fmt.Sprintf(format, a...))
}

func alterDatabaseQueryBuilder(request *pb.AlterDatabaseRequest) (query string, err error) {
	if request.GetDatabaseName() == "" {
		return failBuildQuery("no db name")
	} else {
		return fmt.Sprintf(
			"ALTER DATABASE %s %s;",
			request.GetDatabaseName(),
			readOnlyQueryPartBuilder(request.GetReadOnly()),
		), nil
	}
}
func alterTableQueryBuilder(request *pb.AlterTableRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else if len(request.GetOptions()) == 0 {
		return failBuildQuery("table option is empty")
	} else {
		query_parts := []string{}

		for _, option := range request.GetOptions() {
			var query_part string
			switch option.GetType() {
			case pb.AlterTableOptionType_ADD_COLUMN:
				query_part, err = addColumnQueryPartBuilder(option.GetAddColumn())
			case pb.AlterTableOptionType_ADD_PRIMARY_KEY:
				query_part, err = addPrimaryKeyQueryPartBuilder(option.GetAddPrimaryKey())
			case pb.AlterTableOptionType_ADD_UNIQUE_KEY:
				query_part, err = addUniqueKeyQueryPartBuilder(option.GetAddUniqueKey())
			case pb.AlterTableOptionType_ADD_FOREIGN_KEY:
				query_part, err = addForeignKeyQueryPartBuilder(option.GetAddForeignKey())
			case pb.AlterTableOptionType_CHANGE:
				query_part, err = changeQueryPartBuilder(option.GetChange())
			case pb.AlterTableOptionType_DROP_COLUMN:
				query_part, err = dropColumnQueryPartBuilder(option.GetDropColumn())
			case pb.AlterTableOptionType_DROP_PRIMARY_KEY:
				query_part, err = dropPrimaryKeyQueryPartBuilder(option.GetDropPrimaryKey())
			case pb.AlterTableOptionType_DROP_FOREIGN_KEY:
				query_part, err = dropForeignKeyQueryPartBuilder(option.GetDropForeignKey())
			case pb.AlterTableOptionType_MODIFY:
				query_part, err = modifyQueryPartBuilder(option.GetModify())
			case pb.AlterTableOptionType_ORDER:
				query_part, err = orderQueryPartBuilder(option.GetOrder())
			case pb.AlterTableOptionType_RENAME_COLUMN:
				query_part, err = renameColumnQueryPartBuilder(option.GetRenameColumn())
			case pb.AlterTableOptionType_RENAME:
				query_part, err = renameQueryPartBuilder(option.GetRename())
			case pb.AlterTableOptionType_DROP_KEY:
				query_part, err = dropKeyQueryPartBuilder(option.GetDropKey())
			default:
				return failBuildQuery("unknown option type passed")
			}
			if err != nil {
				return "", err
			} else {
				query_parts = append(query_parts, query_part)
			}
		}

		return fmt.Sprintf("ALTER TABLE %s %s;", request.GetTableName(), strings.Join(query_parts, ", ")), nil
	}
}
func createDatabaseQueryBuilder(request *pb.CreateDatabaseRequest) (query string, err error) {
	if request.GetDatabaseName() == "" {
		return failBuildQuery("no db name")
	} else {
		return fmt.Sprintf("CREATE DATABASE %s;", request.GetDatabaseName()), nil
	}
}
func createTableQueryBuilder(request *pb.CreateTableRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else if len(request.GetOptions()) == 0 {
		return failBuildQuery("table options is empty")
	} else {
		query_parts := []string{}

		for _, option := range request.GetOptions() {
			var query_part string
			switch option.GetType() {
			case pb.CreateTableOptionType_COLUMN:
				query_part, err = columnQueryPartBuilder(option.GetColumn())
			case pb.CreateTableOptionType_PRIMARY_KEY:
				query_part, err = primaryKeyQueryPartBuilder(option.GetPrimaryKey())
			case pb.CreateTableOptionType_UNIQUE_KEY:
				query_part, err = uniqueKeyQueryPartBuilder(option.GetUniqueKey())
			case pb.CreateTableOptionType_FOREIGN_KEY:
				query_part, err = foreignKeyQueryPartBuilder(option.GetForeignKey())
			default:
				return failBuildQuery("unknown option type passed")
			}
			if err != nil {
				return "", err
			} else {
				query_parts = append(query_parts, query_part)
			}
		}

		return fmt.Sprintf("CREATE TABLE %s(%s);", request.GetTableName(), strings.Join(query_parts, ", ")), nil
	}
}
func dropDatabaseQueryBuilder(request *pb.DropDatabaseRequest) (query string, err error) {
	if request.GetDatabaseName() == "" {
		return failBuildQuery("no db name")
	} else {
		return fmt.Sprintf("DROP DATABASE %s;", request.GetDatabaseName()), nil
	}
}
func dropTableQueryBuilder(request *pb.DropTableRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else {
		return fmt.Sprintf("DROP TABLE %s;", request.GetTableName()), nil
	}
}
func renameTableQueryBuilder(request *pb.RenameTableRequest) (query string, err error) {
	if request.GetOldTableName() == "" {
		return failBuildQuery("no old table name")
	} else if request.GetNewTableName() == "" {
		return failBuildQuery("no new table name")
	} else {
		return fmt.Sprintf("RENAME TABLE %s TO %s;", request.GetOldTableName(), request.GetNewTableName()), nil
	}
}
func truncateTableQueryBuilder(request *pb.TruncateTableRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else {
		return fmt.Sprintf("TRUNCATE TABLE %s;", request.GetTableName()), nil
	}
}
