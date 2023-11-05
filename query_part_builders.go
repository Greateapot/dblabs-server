package main

import (
	"fmt"
	"strings"

	pb "greateapot.re/dblabs-api"
)

func failBuildQueryPart(format string, a ...any) (query_part string, err error) {
	return "", fmt.Errorf("error while building query part: %s", fmt.Sprintf(format, a...))
}

func joinColumnListQueryPartBuilder(join_column_list *pb.JoinColumnList) (query_part string, err error) {
	if join_column_list == nil {
		return failBuildQueryPart("no join col list data")
	} else if len(join_column_list.GetColumnNames()) == 0 {
		return failBuildQueryPart("join col list is empty")
	} else {
		return strings.Join(join_column_list.GetColumnNames(), ", "), nil
	}
}
func joinSpecificationQueryPartBuilder(join_specification *pb.JoinSpecification) (query_part string, err error) {
	if join_specification == nil {
		return failBuildQueryPart("no join spec data")
	} else {
		switch join_specification.GetType() {
		case pb.JoinSpecificationType_ON:
			if join_specification.GetSearchCondition() == "" {
				return failBuildQueryPart("no search condition")
			} else {
				query_part = "ON " + join_specification.GetSearchCondition()
			}
		case pb.JoinSpecificationType_USING:
			var join_column_list string
			if join_column_list, err = joinColumnListQueryPartBuilder(join_specification.GetJoinColumnList()); err != nil {
				return "", err
			} else {
				query_part += fmt.Sprintf("USING (%s)", join_column_list)
			}
		}
		return
	}
}
func rowConstructorListQueryPartBuilder(row_constructor_list *pb.RowConstructorList) (query_part string, err error) {
	if row_constructor_list == nil {
		return failBuildQuery("no row constructor list data")
	} else if len(row_constructor_list.GetValueList()) == 0 {
		return failBuildQueryPart("row constructor list value list is empty")
	} else {
		value_lists := []string{}

		for _, v := range row_constructor_list.GetValueList() {
			var value_list string
			if value_list, err = valueListQueryPartBuilder(v); err != nil {
				return "", err
			} else {
				value_lists = append(value_lists, fmt.Sprintf("ROW(%s)", value_list))
			}
		}

		return strings.Join(value_lists, ", "), nil
	}
}
func orderByQueryPartBuilder(order_by *pb.OrderBy) (query_part string, err error) {
	if order_by == nil {
		return failBuildQueryPart("no order by data")
	} else if order_by.GetExpr() == "" {
		return failBuildQueryPart("no order by expr")
	} else {
		query_part = "ORDER BY " + order_by.GetExpr()
		if order_by.GetOrderByDescending() {
			query_part += " DESC"
		}
		return
	}
}
func valueQueryPartBuilder(value *pb.Value) (query_part string, err error) {
	if value == nil {
		return failBuildQueryPart("no value data")
	} else {
		switch value.GetType() {
		case pb.ValueType_VALUE_DEFAULT:
			return "DEFAULT", nil
		case pb.ValueType_VALUE_EXPR:
			if value.GetExpr() == "" {
				return failBuildQueryPart("no value expr")
			} else {
				return value.GetExpr(), nil
			}
		default:
			return failBuildQueryPart("unknown assignment type")
		}
	}
}
func valueListQueryPartBuilder(value_list *pb.ValueList) (query_part string, err error) {
	if value_list == nil {
		return failBuildQueryPart("no value list data")
	} else if len(value_list.GetValues()) == 0 {
		return failBuildQueryPart("value list is empty")
	} else {
		values := []string{}

		for _, value := range value_list.GetValues() {
			var v string
			if v, err = valueQueryPartBuilder(value); err != nil {
				return "", err
			} else {
				values = append(values, v)
			}
		}

		return strings.Join(values, ", "), nil
	}
}
func assignmentListQueryPartBuilder(assignment_list *pb.AssignmentList) (query_part string, err error) {
	if assignment_list == nil {
		return failBuildQueryPart("no assignment list data")
	} else if len(assignment_list.GetAssignments()) == 0 {
		return failBuildQueryPart("assignment list is empty")
	} else {
		assignments := []string{}

		for _, assignment := range assignment_list.GetAssignments() {
			var value string
			if assignment.GetColumnName() == "" {
				return failBuildQueryPart("no assignment col name")
			} else if assignment.GetValue() == nil {
				return failBuildQueryPart("no assignment value")
			} else if value, err = valueQueryPartBuilder(assignment.GetValue()); err != nil {
				return "", err
			} else {
				assignments = append(assignments, fmt.Sprintf("%s = %s", assignment.GetColumnName(), value))
			}
		}

		return strings.Join(assignments, ", "), nil
	}
}
func dropKeyQueryPartBuilder(drop_key *pb.DropKey) (query_part string, err error) {
	if drop_key == nil {
		return failBuildQueryPart("no drop key data")
	} else if drop_key.GetKeyName() == "" {
		return failBuildQueryPart("no key name")
	} else {
		return fmt.Sprintf("DROP KEY %s", drop_key.GetKeyName()), nil
	}
}
func dropColumnQueryPartBuilder(drop_column *pb.DropColumn) (query_part string, err error) {
	if drop_column == nil {
		return failBuildQueryPart("no drop col data")
	} else if drop_column.GetColumnName() == "" {
		return failBuildQueryPart("no col name")
	} else {
		return fmt.Sprintf("DROP COLUMN %s", drop_column.GetColumnName()), nil
	}
}
func dropPrimaryKeyQueryPartBuilder(drop_primary_key *pb.DropPrimaryKey) (query_part string, err error) {
	if drop_primary_key == nil {
		return failBuildQueryPart("no drop pk data") // lol, its empty
	} else {
		return "DROP PRIMARY KEY", nil
	}
}
func dropForeignKeyQueryPartBuilder(drop_foreign_key *pb.DropForeignKey) (query_part string, err error) {
	if drop_foreign_key == nil {
		return failBuildQueryPart("no drop fk data")
	} else if drop_foreign_key.GetForeignKeySymbol() == "" {
		return failBuildQueryPart("no fk symbol")
	} else {
		return fmt.Sprintf("DROP FOREIGN KEY %s", drop_foreign_key.GetForeignKeySymbol()), nil
	}
}
func modifyQueryPartBuilder(modify *pb.Modify) (query_part string, err error) {
	if modify == nil {
		return failBuildQueryPart("no modify data")
	} else if column, err := columnQueryPartBuilder(modify.GetColumn()); err != nil {
		return "", err
	} else if insert, err := insertQueryPartBuilder(modify.GetInsert()); err != nil {
		return "", err
	} else {
		return strings.TrimSpace(fmt.Sprintf("MODIFY COLUMN %s %s", column, insert)), nil
	}
}
func orderQueryPartBuilder(order *pb.Order) (query_part string, err error) {
	if order.GetColumnNames() == nil {
		return failBuildQueryPart("no order data")
	} else if len(order.GetColumnNames()) == 0 {
		return failBuildQueryPart("col names is empty")
	} else {
		return fmt.Sprintf("ORDER BY %s", strings.Join(order.GetColumnNames(), ", ")), nil
	}
}
func renameColumnQueryPartBuilder(rename_column *pb.RenameColumn) (query_part string, err error) {
	if rename_column == nil {
		return failBuildQueryPart("no rename col data")
	} else if rename_column.GetOldColumnName() == "" {
		return failBuildQueryPart("no old col name")
	} else if rename_column.GetNewColumnName() == "" {
		return failBuildQueryPart("no new col name")
	} else {
		return fmt.Sprintf("RENAME COLUMN %s TO %s", rename_column.GetOldColumnName(), rename_column.GetNewColumnName()), nil
	}
}
func renameQueryPartBuilder(rename *pb.Rename) (query_part string, err error) {
	if rename == nil {
		return failBuildQueryPart("no rename data")
	} else if rename.GetNewTableName() == "" {
		return failBuildQueryPart("no new table name")
	} else {
		return fmt.Sprintf("RENAME TO %s", rename.GetNewTableName()), nil
	}
}
func changeQueryPartBuilder(change *pb.Change) (query_part string, err error) {
	if change == nil {
		return failBuildQueryPart("no change data")
	} else if change.GetOldColumnName() == "" {
		return failBuildQueryPart("no change old col name")
	} else if column, err := columnQueryPartBuilder(change.GetNewColumn()); err != nil {
		return "", err
	} else if insert, err := insertQueryPartBuilder(change.GetInsert()); err != nil {
		return "", err
	} else {
		return strings.TrimSpace(fmt.Sprintf("CHANGE %s %s %s", change.GetOldColumnName(), column, insert)), nil
	}
}
func addColumnQueryPartBuilder(add_column *pb.AddColumn) (query_part string, err error) {
	if add_column == nil {
		return failBuildQueryPart("no add col data")
	} else if column, err := columnQueryPartBuilder(add_column.GetColumn()); err != nil {
		return "", err
	} else if insert, err := insertQueryPartBuilder(add_column.GetInsert()); err != nil {
		return "", err
	} else {
		return strings.TrimSpace(fmt.Sprintf("ADD COLUMN %s %s", column, insert)), nil
	}
}
func addPrimaryKeyQueryPartBuilder(add_primary_key *pb.AddPrimaryKey) (query_part string, err error) {
	if add_primary_key == nil {
		return failBuildQueryPart("no add pk data")
	} else if pk, err := primaryKeyQueryPartBuilder(add_primary_key.GetPrimaryKey()); err != nil {
		return "", err
	} else {
		return fmt.Sprintf("ADD %s", pk), nil
	}
}
func addUniqueKeyQueryPartBuilder(add_primary_key *pb.AddUniqueKey) (query_part string, err error) {
	if add_primary_key == nil {
		return failBuildQueryPart("no add pk data")
	} else if uk, err := uniqueKeyQueryPartBuilder(add_primary_key.GetUniqueKey()); err != nil {
		return "", err
	} else {
		return fmt.Sprintf("ADD %s", uk), nil
	}
}
func addForeignKeyQueryPartBuilder(add_primary_key *pb.AddForeignKey) (query_part string, err error) {
	if add_primary_key == nil {
		return failBuildQueryPart("no add pk data")
	} else if fk, err := foreignKeyQueryPartBuilder(add_primary_key.GetForeignKey()); err != nil {
		return "", err
	} else {
		return fmt.Sprintf("ADD %s", fk), nil
	}
}
func insertQueryPartBuilder(insert *pb.InsertColumn) (query_part string, err error) {
	if insert != nil {
		switch insert.GetType() {
		case pb.InsertColumnType_FIRST:
			query_part = "FIRST"
		case pb.InsertColumnType_AFTER:
			if after_column_name := insert.GetAfterColumnName(); after_column_name == "" {
				return failBuildQueryPart("no col name for insert after option")
			} else {
				query_part = fmt.Sprintf("AFTER %s", after_column_name)
			}
		}
	}
	return
}
func readOnlyQueryPartBuilder(ro pb.ReadOnly) string {
	switch ro {
	case pb.ReadOnly_ENABLED:
		return "READ ONLY = 1"
	case pb.ReadOnly_DISABLED:
		return "READ ONLY = 0"
	default:
		return "READ ONLY = DEFAULT"
	}
}
func foreignKeyQueryPartBuilder(fk *pb.ForeignKey) (query_part string, err error) {
	if fk == nil {
		return failBuildQueryPart("no fk data")
	} else if len(fk.GetColumnNames()) == 0 {
		return failBuildQueryPart("fk col names is empty")
	} else if len(fk.GetParentKeyParts()) == 0 {
		return failBuildQueryPart("fk key parts is empty")
	} else if len(fk.GetColumnNames()) != len(fk.GetParentKeyParts()) {
		return failBuildQueryPart("fk col names len nq key parts len")
	} else if fk.GetParentTableName() == "" {
		return failBuildQueryPart("no fk parent table name")
	}
	return fmt.Sprintf(
		"CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		fk.GetConstraintSymbol(),
		strings.Join(fk.GetColumnNames(), ", "),
		fk.GetParentTableName(),
		strings.Join(fk.GetParentKeyParts(), ", "),
	), nil
}
func uniqueKeyQueryPartBuilder(uk *pb.UniqueKey) (query_part string, err error) {
	if uk == nil {
		return failBuildQueryPart("no uk data")
	} else if len(uk.GetKeyParts()) == 0 {
		return failBuildQueryPart("uk key parts is empty")
	}
	return fmt.Sprintf(
		"CONSTRAINT %s UNIQUE KEY (%s)",
		uk.GetConstraintSymbol(),
		strings.Join(uk.GetKeyParts(), ", "),
	), nil
}
func primaryKeyQueryPartBuilder(pk *pb.PrimaryKey) (query_part string, err error) {
	if pk == nil {
		return failBuildQueryPart("no pk data")
	} else if len(pk.GetKeyParts()) == 0 {
		return failBuildQueryPart("pk key parts is empty")
	}
	return fmt.Sprintf(
		"CONSTRAINT %s PRIMARY KEY (%s)",
		pk.GetConstraintSymbol(),
		strings.Join(pk.GetKeyParts(), ", "),
	), nil
}
func columnQueryPartBuilder(column *pb.Column) (query_part string, err error) {
	if column == nil {
		return failBuildQueryPart("no column data")
	} else if column.GetColumnName() == "" {
		return failBuildQueryPart("no column name")
	} else if column.GetDataType() == nil {
		return failBuildQueryPart("no data type (col: %s)", column.GetColumnName())
	} else {
		query_part = column.GetColumnName() + " " + column.GetDataType().GetType().String()
		switch column.GetDataType().GetType() {
		case pb.DataTypeType_INT,
			pb.DataTypeType_SMALLINT,
			pb.DataTypeType_TINYINT,
			pb.DataTypeType_MEDIUMINT,
			pb.DataTypeType_BIGINT:
			attrs := column.GetDataType().GetIntAttrs()
			if attrs.GetUnsigned() {
				query_part += " UNSIGNED"
			}
			if attrs.GetAutoIncrement() {
				query_part += " AUTO_INCREMENT"
			}
		case pb.DataTypeType_FLOAT:
			if attrs := column.GetDataType().GetFloatAttrs(); attrs != nil {
				if attrs.GetP() != 0 {
					query_part += fmt.Sprintf("(%d)", attrs.GetP())
				}
			} else if attrs := column.GetDataType().GetDoubleAttrs(); attrs != nil {
				if attrs.GetSize() != 0 && attrs.GetD() != 0 {
					query_part += fmt.Sprintf("(%d, %d)", attrs.GetSize(), attrs.GetD())
				} else if attrs.GetSize() != 0 {
					query_part += fmt.Sprintf("(%d)", attrs.GetSize())
				}
			}
		case pb.DataTypeType_DECIMAL,
			pb.DataTypeType_NUMERIC,
			pb.DataTypeType_DOUBLE:
			attrs := column.GetDataType().GetDoubleAttrs()
			if attrs.GetSize() != 0 && attrs.GetD() != 0 {
				query_part += fmt.Sprintf("(%d, %d)", attrs.GetSize(), attrs.GetD())
			} else if attrs.GetSize() != 0 {
				query_part += fmt.Sprintf("(%d)", attrs.GetSize())
			}
		case pb.DataTypeType_DATETIME,
			pb.DataTypeType_TIMESTAMP,
			pb.DataTypeType_TIME:
			attrs := column.GetDataType().GetTimeAttrs()
			if attrs.GetFsp() != 0 {
				query_part += fmt.Sprintf("(%d)", attrs.GetFsp())
			}
		case pb.DataTypeType_CHAR,
			pb.DataTypeType_VARCHAR,
			pb.DataTypeType_BINARY,
			pb.DataTypeType_VARBINARY,
			pb.DataTypeType_BLOB,
			pb.DataTypeType_TEXT:
			attrs := column.GetDataType().GetStringAttrs()
			if attrs.GetSize() != 0 {
				query_part += fmt.Sprintf("(%d)", attrs.GetSize())
			}
		case pb.DataTypeType_ENUM:
			attrs := column.GetDataType().GetEnumAttrs()
			if len(attrs.GetValues()) == 0 {
				return failBuildQueryPart("no enum values (col: %s)", column.GetColumnName())
			} else {
				query_part += fmt.Sprintf("(%s)", strings.Join(attrs.GetValues(), ", "))
			}
		case pb.DataTypeType_BIT,
			pb.DataTypeType_DATE,
			pb.DataTypeType_LONGBLOB,
			pb.DataTypeType_LONGTEXT,
			pb.DataTypeType_MEDIUMBLOB,
			pb.DataTypeType_MEDIUMTEXT,
			pb.DataTypeType_TINYBLOB,
			pb.DataTypeType_TINYTEXT,
			pb.DataTypeType_YEAR:
			break // bypass
		default:
			return failBuildQueryPart("unknown data type (col: %s)", column.GetColumnName())
		}
		if column.GetNotNull() {
			query_part += " NOT NULL"
		}
		if column.GetDefaultValue() != nil {
			query_part += fmt.Sprintf(" DEFAULT %s", column.GetDefaultValue().GetValue())
		}
		return
	}
}
func asQueryPartBuilder(as *pb.As) (query_part string, err error) {
	if as == nil {
		return failBuildQueryPart("no as data")
	} else if as.GetName() == "" {
		return failBuildQueryPart("no as name")
	} else {
		return fmt.Sprintf("AS %s", as.GetName()), nil
	}
}
func likeQueryPartBuilder(like *pb.Like) (query_part string, err error) {
	if like == nil {
		return failBuildQueryPart("no like data")
	} else if like.GetName() == "" {
		return failBuildQueryPart("no like name")
	} else {
		return fmt.Sprintf("LIKE %s", like.GetName()), nil
	}
}
func alterColumnQueryPartBuilder(alter_column *pb.AlterColumn) (query_part string, err error) {
	if alter_column == nil {
		return failBuildQueryPart("no alter col data")
	} else if alter_column.GetColumnName() == "" {
		return failBuildQueryPart("no alter col name")
	} else {
		switch alter_column.GetType() {
		case pb.AlterColumnType_SET_DEFAULT_VALUE:
			if alter_column.GetNewDefaultValue() == nil {
				return failBuildQueryPart("no alter col new def")
			} else {
				return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %v", alter_column.GetColumnName(), alter_column.GetNewDefaultValue()), nil
			}
		case pb.AlterColumnType_DROP_DEFAULT_VALUE:
			return fmt.Sprintf("ALTER COLUMN %s DROP DEFAULT", alter_column.GetColumnName()), nil
		default:
			return failBuildQueryPart("unknown alter col type")
		}
	}
}
func selectDataQueryPartBuilder(select_data *pb.SelectData, json_wrap bool) (query_part string, err error) {
	if select_data == nil {
		return failBuildQueryPart("no select data data (dup, lol)")
	} else if len(select_data.GetColumnNames()) == 0 {
		return failBuildQueryPart("col names is empty")
	} else if select_data.GetTableName() == "" {
		return failBuildQueryPart("no table name")
	} else {
		select_expr := strings.Join(select_data.GetColumnNames(), ", ")
		if json_wrap {
			select_expr = fmt.Sprintf("JSON_ARRAYAGG(JSON_ARRAY(%s))", select_expr)
		}
		query_part = fmt.Sprintf("SELECT %s FROM %s", select_expr, select_data.GetTableName())
		if select_data.GetWhereCondition() != "" {
			query_part += " WHERE " + select_data.GetWhereCondition()
		}
		if select_data.GetGroupByExpr() != "" {
			query_part += " GROUP BY " + select_data.GetGroupByExpr()
		}
		if select_data.GetHavingCondition() != "" {
			query_part += " HAVING " + select_data.GetHavingCondition()
		}
		if select_data.GetOrderBy() != nil {
			var order_by string
			if order_by, err = orderByQueryPartBuilder(select_data.GetOrderBy()); err != nil {
				return "", err
			} else {
				query_part += " " + order_by
			}
		}
		if select_data.GetLimit() != 0 {
			query_part += fmt.Sprintf(" LIMIT %d", select_data.GetLimit())
		}
		return
	}
}
