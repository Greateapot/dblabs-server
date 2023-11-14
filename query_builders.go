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
func showDatabasesQueryBuilder(request *pb.ShowDatabasesRequest) (query string, err error) {
	/*
		select json_arrayagg(json_array(SCHEMA_NAME)) from INFORMATION_SCHEMA.SCHEMATA;
		WHERE SCHEMA_NAME != 'sys' AND SCHEMA_NAME != 'information_schema'
		AND SCHEMA_NAME != 'performance_schema' AND SCHEMA_NAME != 'mysql';
	*/
	where_condition := ""
	if !request.GetShowSys() {
		where_condition = "SCHEMA_NAME != 'sys' AND SCHEMA_NAME != 'information_schema'" +
			"AND SCHEMA_NAME != 'performance_schema' AND SCHEMA_NAME != 'mysql';"
	}
	return selectDataQueryPartBuilder(&pb.SelectData{
		TableName:      "INFORMATION_SCHEMA.SCHEMATA",
		ColumnNames:    []string{"SCHEMA_NAME"},
		WhereCondition: where_condition,
	}, true)
}
func showTablesQueryBuilder(request *pb.ShowTablesRequest) (query string, err error) {
	//  select json_arrayagg(json_array(TABLE_NAME)) from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '%s';
	if request.GetDatabaseName() == "" {
		return failBuildQuery("no db name")
	} else {
		return selectDataQueryPartBuilder(&pb.SelectData{
			TableName:      "INFORMATION_SCHEMA.TABLES",
			ColumnNames:    []string{"TABLE_NAME"},
			WhereCondition: fmt.Sprintf("TABLE_SCHEMA = '%s'", request.GetDatabaseName()),
		}, true)
	}
}
func showTableStructQueryBuilder(request *pb.ShowTableStructRequest) (query string, err error) {
	/*
		SELECT json_arrayagg(json_array(COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA))
		FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';
	*/
	if request.GetDatabaseName() == "" {
		return failBuildQuery("no db name")
	} else if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else {
		return selectDataQueryPartBuilder(&pb.SelectData{
			TableName:   "INFORMATION_SCHEMA.COLUMNS",
			ColumnNames: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "EXTRA"},
			WhereCondition: fmt.Sprintf(
				"TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
				request.GetDatabaseName(),
				request.GetTableName(),
			),
		}, true)
	}
}
func dropTriggerQueryBuilder(request *pb.DropTriggerRequest) (query string, err error) {
	if request.GetTriggerName() == "" {
		return failBuildQuery("no trigger name")
	} else {
		return fmt.Sprintf("DROP TRIGGER %s", request.GetTriggerName()), nil
	}
}
func createTriggerQueryBuilder(request *pb.CreateTriggerRequest) (query string, err error) {
	// CREATE TRIGGER trigger_name trigger_time trigger_event ON tbl_name FOR EACH ROW [trigger_order] trigger_body
	if request.GetTriggerName() == "" {
		return failBuildQuery("no trigger name")
	} else if request.GetTableName() == "" {
		return failBuildQuery("no table name")
	} else if request.GetTriggerBody() == "" {
		return failBuildQuery("no trigger body")
	} else {
		trigger_order := ""
		if request.GetTriggerOrder() != nil {
			if trigger_order, err = triggerOrderQueryPartBuilder(request.GetTriggerOrder()); err != nil {
				return "", err
			}
		}
		return fmt.Sprintf(
			"CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s BEGIN %s END",
			request.GetTriggerName(),
			strings.Split(request.GetTriggerTime().String(), "_")[0], // proto-кастыли
			request.GetTriggerEvent().String(),
			request.GetTableName(),
			trigger_order,
			request.GetTriggerBody(),
		), nil
	}
}
func createViewQueryBuilder(request *pb.CreateViewRequest) (query string, err error) {
	if request.GetViewName() == "" {
		return failBuildQuery("no view name")
	} else {
		var selectData, orReplace string
		if selectData, err = selectDataQueryPartBuilder(request.GetSelectData(), false); err != nil {
			return "", err
		}
		if request.GetOrReplace() {
			orReplace = "OR REPLACE"
		} else {
			orReplace = ""
		}
		algorithm := viewAlgorithmTypeQueryPartBuilder(request.GetAlgorithm())
		withCheckOption := viewWithCheckOptionTypeQueryPartBuilder(request.GetWithCheckOption())
		columnNames := strings.Join(request.GetColumnList(), ", ")
		if columnNames != "" {
			columnNames = fmt.Sprintf("(%s)", columnNames)
		}
		return fmt.Sprintf(
			"CREATE %s %s VIEW %s %s AS %s %s",
			orReplace,
			algorithm,
			request.GetViewName(),
			columnNames,
			selectData,
			withCheckOption,
		), nil
	}
}
func alterViewQueryBuilder(request *pb.AlterViewRequest) (query string, err error) {
	if request.GetViewName() == "" {
		return failBuildQuery("no view name")
	} else {
		var selectData string
		if selectData, err = selectDataQueryPartBuilder(request.GetSelectData(), false); err != nil {
			return "", err
		}
		algorithm := viewAlgorithmTypeQueryPartBuilder(request.GetAlgorithm())
		withCheckOption := viewWithCheckOptionTypeQueryPartBuilder(request.GetWithCheckOption())
		columnNames := strings.Join(request.GetColumnList(), ", ")
		if columnNames != "" {
			columnNames = fmt.Sprintf("(%s)", columnNames)
		}
		return fmt.Sprintf(
			"ALTER %s VIEW %s %s AS %s %s",
			algorithm,
			request.GetViewName(),
			columnNames,
			selectData,
			withCheckOption,
		), nil
	}
}
func dropViewQueryBuilder(request *pb.DropViewRequest) (query string, err error) {
	if request.GetViewName() == "" {
		return failBuildQuery("no view name")
	} else {
		return fmt.Sprintf("DROP VIEW %s", request.GetViewName()), nil
	}
}
func createProcedureQueryBuilder(request *pb.CreateProcedureRequest) (query string, err error) {
	if request.GetProcedureName() == "" {
		return failBuildQuery("no procedure name")
	} else if request.GetRoutineBody() == "" {
		return failBuildQuery("no procedure routine body")
	} else {
		query = "CREATE PROCEDURE " + request.GetProcedureName()
		if len(request.GetProcedureParameters()) > 0 {
			pps := []string{}
			for _, procedure_parameter := range request.GetProcedureParameters() {
				var pp string
				if pp, err = procedureParameterQueryPartBuilder(procedure_parameter); err != nil {
					return "", err
				} else {
					pps = append(pps, pp)
				}
			}
			query += fmt.Sprintf(" (%s)", strings.Join(pps, ", "))
		}
		query += fmt.Sprintf(" BEGIN %s END", request.GetRoutineBody())
		return
	}
}
func dropProcedureQueryBuilder(request *pb.DropProcedureRequest) (query string, err error) {
	if request.GetProcedureName() == "" {
		return failBuildQuery("no view name")
	} else {
		return fmt.Sprintf("DROP PROCEDURE %s", request.GetProcedureName()), nil
	}
}
func setQueryBuilder(request *pb.SetRequest) (query string, err error) {
	if request.GetVarName() == "" {
		return failBuildQuery("no set var name")
	} else if request.GetExpr() == "" {
		return failBuildQuery("no set expr")
	} else {
		return fmt.Sprintf("SET %s = %s", request.GetVarName(), request.GetExpr()), nil
	}
}
func callProcedureQueryBuilder(request *pb.CallProcedureRequest) (query string, err error) {
	if request.GetExpr() == "" {
		return failBuildQuery("no expr")
	} else {
		return fmt.Sprintf("CALL %s", request.GetExpr()), nil
	}
}
