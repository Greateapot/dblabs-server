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
			case pb.AlterTableOptionType_ALTER_COLUMN:
				query_part, err = alterColumnQueryPartBuilder(option.GetAlterColumn())
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
			case pb.CreateTableOptionType_AS:
				query_part, err = asQueryPartBuilder(option.GetAs())
			case pb.CreateTableOptionType_LIKE:
				query_part, err = likeQueryPartBuilder(option.GetLike())
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
func deleteQueryBuilder(request *pb.DeleteRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else {
		query = "DELETE FROM " + request.GetTableName()
		if request.GetTableAlias() != "" {
			query += " AS " + request.GetTableAlias()
		}
		if request.GetWhereCondition() != "" {
			query += " WHERE " + request.GetWhereCondition()
		}
		if request.GetOrderBy() != nil {
			var order_by string
			if order_by, err = orderByQueryPartBuilder(request.GetOrderBy()); err != nil {
				return "", err
			} else {
				query += " " + order_by
			}
		}
		if request.GetLimit() != 0 {
			query += fmt.Sprintf(" LIMIT %d", request.GetLimit())
		}
		return
	}
}
func updateQueryBuilder(request *pb.UpdateRequest) (query string, err error) {
	var assignments string
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else if assignments, err = assignmentListQueryPartBuilder(request.GetAssignmentList()); err != nil {
		return "", err
	} else {
		query = fmt.Sprintf("UPDATE %s SET %s", request.GetTableName(), assignments)
		if request.GetWhereCondition() != "" {
			query += " WHERE " + request.GetWhereCondition()
		}
		if request.GetOrderBy() != nil {
			var order_by string
			if order_by, err = orderByQueryPartBuilder(request.GetOrderBy()); err != nil {
				return "", err
			} else {
				query += " " + order_by
			}
		}
		if request.GetLimit() != 0 {
			query += fmt.Sprintf(" LIMIT %d", request.GetLimit())
		}
		return
	}
}
func insertQueryBuilder(request *pb.InsertRequest) (query string, err error) {
	if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else {
		query = "INSERT INTO " + request.GetTableName()
		if len(request.GetColumnNames()) > 0 {
			query += fmt.Sprintf("(%s)", strings.Join(request.GetColumnNames(), ", "))
		}
		switch request.GetInsertType() {
		case pb.InsertType_SELECT:
			var select_data string
			if select_data, err = selectDataQueryPartBuilder(request.GetSelectData(), false); err != nil {
				return "", err
			} else {
				query += " " + select_data
			}
		case pb.InsertType_TABLE:
			if request.GetOtherTableName() == "" {
				return failBuildQuery("no other table name")
			} else {
				query += " TABLE " + request.GetOtherTableName()
			}
		case pb.InsertType_VALUES:
			var values string
			if values, err = rowConstructorListQueryPartBuilder(request.GetRowConstructorList()); err != nil {
				return "", err
			} else {
				query += " VALUES " + values
			}
		default:
			return failBuildQuery("unknown insert type")
		}
		if request.GetOnDuplicateKeyUpdate() != nil {
			var assignment_list string
			if assignment_list, err = assignmentListQueryPartBuilder(request.GetOnDuplicateKeyUpdate()); err != nil {
				return "", nil
			} else {
				query += " ON DUPLICATE KEY UPDATE " + assignment_list
			}
		}
		return
	}
}
func selectQueryBuilder(request *pb.SelectRequest) (query string, err error) {
	return selectDataQueryPartBuilder(request.GetSelectData(), true)
}
func joinQueryBuilder(request *pb.JoinRequest) (query string, err error) {
	if len(request.GetColumnNames()) == 0 {
		return failBuildQuery("col names is empty")
	} else if request.GetFirstTableName() == "" {
		return failBuildQuery("no first table name")
	} else if request.GetSecondTableName() == "" {
		return failBuildQuery("no second table name")
	} else if request.GetJoin() == nil {
		return failBuildQuery("no join data")
	} else {
		is_join_specification_required := false
		query = fmt.Sprintf("SELECT JSON_ARRAYAGG(JSON_ARRAY(%s)) FROM %s", strings.Join(request.GetColumnNames(), ", "), request.GetFirstTableName())
		if request.GetFirstTableAlias() != "" {
			query += " AS " + request.GetFirstTableAlias()
		}
		switch request.GetJoin().GetJoinType() {
		case pb.JoinType_INNER, pb.JoinType_CROSS:
			query += fmt.Sprintf(" %s JOIN", request.GetJoin().GetJoinType().String())
		case pb.JoinType_LEFT, pb.JoinType_RIGHT:
			is_join_specification_required = true
			query += fmt.Sprintf(" %s OUTER JOIN", request.GetJoin().GetJoinType().String())
		default:
			return failBuildQuery("unknown join type")
		}
		query += " " + request.GetSecondTableName()
		if request.GetSecondTableAlias() != "" {
			query += " AS " + request.GetSecondTableAlias()
		}
		if is_join_specification_required && request.GetJoin().GetJoinSpecification() == nil {
			return failBuildQuery("no join spec for this join type")
		} else if request.GetJoin().GetJoinSpecification() != nil {
			var join_specification string
			if join_specification, err = joinSpecificationQueryPartBuilder(request.Join.GetJoinSpecification()); err != nil {
				return "", err
			} else {
				query += " " + join_specification
			}
		}
		if request.GetWhereCondition() != "" {
			query += " WHERE " + request.GetWhereCondition()
		}
		if request.GetOrderBy() != nil {
			var order_by string
			if order_by, err = orderByQueryPartBuilder(request.GetOrderBy()); err != nil {
				return "", err
			} else {
				query += " " + order_by
			}
		}
		return
	}
}
