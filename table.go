package rawsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/parser/types"
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
)

type Table struct {
	ColumnTypes []gorm.ColumnType
	Indexes     []gorm.Index
	Name        string
	Comment     string
}

type Parser interface {
	ParseSQL(sql string) error
	GetTables() map[string]*Table
}

type defaultParser struct {
	tables map[string]*Table
}

func newDefaultParse() Parser {
	return &defaultParser{tables: make(map[string]*Table)}
}

func (d *defaultParser) GetTables() map[string]*Table {
	return d.tables
}

func (d *defaultParser) ParseSQL(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}

	for _, node := range stmtNodes {
		switch node.(type) {
		case *ast.CreateTableStmt:
			create := node.(*ast.CreateTableStmt)

			tableName := create.Table.Name.String()

			if _, has := d.tables[tableName]; has {
				panic(fmt.Sprintf("duplicated table %s", tableName))
			}

			d.tables[tableName] = &Table{
				Name:        tableName,
				Comment:     create.Table.TableInfo.Comment,
				ColumnTypes: d.getColumnTypes(create),
				Indexes:     d.getIndexes(create),
			}
		case *ast.AlterTableStmt:
			alter := node.(*ast.AlterTableStmt)

			tableName := alter.Table.Name.String()

			table, has := d.tables[tableName]
			if !has {
				panic(fmt.Sprintf("table %s not exists", tableName))
			}

			for _, spec := range alter.Specs {
				if spec.OldColumnName != nil {
					cols := table.ColumnTypes
					for i, v := range cols {
						if v.Name() == spec.OldColumnName.Name.String() {
							table.ColumnTypes = append(cols[:i], cols[i+1:]...)
						}
					}
				}

				for _, v := range spec.NewColumns {
					ct := d.getColumnType(v)

					for i, existColumn := range table.ColumnTypes {
						if existColumn.Name() == v.Name.String() {
							// remove duplicate column
							table.ColumnTypes = append(
								table.ColumnTypes[:i],
								table.ColumnTypes[i+1:]...,
							)

							break
						}
					}

					position := -1
					if spec.Position != nil {
						switch spec.Position.Tp {
						case ast.ColumnPositionFirst:
							position = 0
						case ast.ColumnPositionAfter:
							for i, existColumn := range table.ColumnTypes {
								if existColumn.Name() == spec.Position.RelativeColumn.Name.String() {
									position = i + 1
								}
							}
						}
					}

					if position > 0 {
						table.ColumnTypes = append(
							table.ColumnTypes[:position],
							append(
								[]gorm.ColumnType{ct}, table.ColumnTypes[position:]...,
							)...,
						)
					} else {
						table.ColumnTypes = append(table.ColumnTypes, ct)
					}
				}
			}
		case *ast.DropTableStmt:
			drop := node.(*ast.DropTableStmt)

			for _, table := range drop.Tables {
				if _, has := d.tables[table.Name.String()]; !has && !drop.IfExists {
					panic(fmt.Sprintf("table %s not exists", table.Name.String()))
				}

				delete(d.tables, table.Name.String())
			}
		}
	}

	return nil
}

func (d *defaultParser) getColumnTypes(create *ast.CreateTableStmt) (cols []gorm.ColumnType) {
	if create == nil || len(create.Cols) == 0 {
		return nil
	}

	var primaryConstraint *ast.Constraint
	for _, constraint := range create.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			primaryConstraint = constraint
		}
	}

	cols = make([]gorm.ColumnType, 0, len(create.Cols))
	for _, col := range create.Cols {
		ct := d.getColumnType(col)

		if primaryConstraint != nil {
			for _, pk := range primaryConstraint.Keys {
				if pk.Column.Name.String() == ct.Name() {
					ct.(*migrator.ColumnType).PrimaryKeyValue = sql.NullBool{
						Bool:  true,
						Valid: true,
					}
				}
			}
		}

		cols = append(cols, ct)
	}

	return cols
}

func (*defaultParser) getColumnType(col *ast.ColumnDef) gorm.ColumnType {
	ct := &migrator.ColumnType{
		NameValue: sql.NullString{Valid: true, String: col.Name.OrigColName()},
		DataTypeValue: sql.NullString{
			Valid:  true,
			String: strings.ToLower(types.TypeToStr(col.Tp.GetType(), col.Tp.GetCharset())),
		},
		ColumnTypeValue: sql.NullString{Valid: true, String: strings.ToLower(col.Tp.String())},
		PrimaryKeyValue: sql.NullBool{
			Bool:  mysql.HasPriKeyFlag(col.Tp.GetFlag()),
			Valid: mysql.HasPriKeyFlag(col.Tp.GetFlag()),
		},
		UniqueValue: sql.NullBool{
			Bool:  mysql.HasUniKeyFlag(col.Tp.GetFlag()),
			Valid: mysql.HasUniKeyFlag(col.Tp.GetFlag()),
		},
		LengthValue:      sql.NullInt64{Int64: int64(col.Tp.GetFlen()), Valid: col.Tp.IsVarLengthType()},
		DecimalSizeValue: sql.NullInt64{Int64: int64(col.Tp.GetFlen()), Valid: col.Tp.IsDecimalValid()},
		ScaleValue:       sql.NullInt64{Int64: int64(col.Tp.GetDecimal()), Valid: col.Tp.IsDecimalValid()},
		NullableValue:    sql.NullBool{Bool: true, Valid: true},
		SQLColumnType:    &sql.ColumnType{},
		ScanTypeValue:    getType(col.Tp),
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionNotNull {
			ct.NullableValue.Bool = false
			continue
		}
		if opt.Tp == ast.ColumnOptionComment {
			ct.CommentValue = sql.NullString{
				String: opt.Expr.(*test_driver.ValueExpr).Datum.GetString(),
				Valid:  true,
			}
			continue
		}
		if opt.Tp == ast.ColumnOptionAutoIncrement {
			ct.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
			continue
		}
		if opt.Tp == ast.ColumnOptionDefaultValue {
			if v, ok := opt.Expr.(*test_driver.ValueExpr); ok {
				dv := sql.NullString{
					Valid: true,
				}
				switch v.Datum.Kind() {
				case test_driver.KindInt64:
					dv.String = strconv.FormatInt(v.Datum.GetInt64(), 10)
				case test_driver.KindUint64:
					dv.String = strconv.FormatUint(v.Datum.GetUint64(), 10)
				default:
					dv.String = v.Datum.GetString()
				}

				ct.DefaultValueValue = dv

				continue
			}

			if v2, ok := opt.Expr.(*ast.FuncCallExpr); ok {
				ct.DefaultValueValue = sql.NullString{Valid: true, String: v2.FnName.String()}
			}
		}

		if opt.Tp == ast.ColumnOptionPrimaryKey {
			ct.PrimaryKeyValue = sql.NullBool{
				Valid: true,
				Bool:  true,
			}
		}
	}

	return ct
}

func (d *defaultParser) getIndexes(create *ast.CreateTableStmt) []gorm.Index {
	if create == nil || len(create.Constraints) == 0 {
		return nil
	}
	indexs := make([]gorm.Index, 0, len(create.Constraints))
	table := create.Table.Name.String()
	for _, cons := range create.Constraints {
		idx := &migrator.Index{
			TableName: table, NameValue: cons.Name, ColumnList: []string{},
			PrimaryKeyValue: sql.NullBool{
				Bool:  ast.ConstraintPrimaryKey == cons.Tp,
				Valid: ast.ConstraintPrimaryKey == cons.Tp,
			},
			UniqueValue: sql.NullBool{Bool: ast.ConstraintUniq == cons.Tp, Valid: ast.ConstraintUniq == cons.Tp},
		}
		for _, col := range cons.Keys {
			idx.ColumnList = append(idx.ColumnList, col.Column.Name.String())
		}
		indexs = append(indexs, idx)
	}
	return indexs
}

var (
	intT    = reflect.TypeOf(int32(0))
	longT   = reflect.TypeOf(int64(0))
	boolT   = reflect.TypeOf(false)
	stringT = reflect.TypeOf("")
	floatT  = reflect.TypeOf(float32(0))
	doubleT = reflect.TypeOf(float64(0))
	timeT   = reflect.TypeOf(time.Time{})
)

func getType(tp *types.FieldType) reflect.Type {
	if tp == nil {
		return nil
	}
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong:
		return intT
	case mysql.TypeFloat:
		return floatT
	case mysql.TypeDouble:
		return doubleT
	case mysql.TypeTimestamp, mysql.TypeLonglong, mysql.TypeInt24:
		return longT
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate:
		return timeT
	default:
		return stringT
	}
}
