package strikemysqlqueries

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var numericRegexpWithColon = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

//FieldWithValue is the structure used to generate a field with a name and a value.
//Eg.: FieldWithValue	{FieldName:"name",		FieldValue:"Jhon"	}
//Eg.: FieldWithValue	{FieldName:"age",		FieldValue:12		}
//Eg.: FieldWithValue	{FieldName:"is_adult",	FieldValue:false	}
//This struct makes generating the SQL query easier and therefore is simple but necessary
type FieldWithValue struct {
	FieldName  string
	FieldValue interface{}
}

func (fwv *FieldWithValue) print(table string) string {
	if table == "" || strings.Contains(fwv.FieldName, ".") {
		return fmt.Sprintf("%s = %s", fwv.FieldName, fwv.printValue())
	}
	return fmt.Sprintf("%s.%s = %s", table, fwv.FieldName, fwv.printValue())
}

func (fwv *FieldWithValue) printValue() string {
	fwvToString := fmt.Sprintf("%v", fwv.FieldValue)
	if fwv.FieldValue == nil || fwvToString == "" || !isPrimitiveType(fwv.FieldValue) {
		return ""
	}
	if isNumericString(fwvToString) || isBooleanString(fwvToString) {
		return fwvToString
	}
	if t, ok := fwv.FieldValue.(time.Time); ok {
		fwvToString = t.UTC().Format("2006-01-02 15:04:05")
	}
	return fmt.Sprintf("'%s'", fwvToString)
}

func isStringTime() bool {
	return true
}

func isBooleanString(inputString string) bool {
	return inputString == "true" || inputString == "false"
}

func isNumericString(inputString string) bool {
	return numericRegexpWithColon.Match([]byte(inputString))
}

func isPrimitiveType(inputType interface{}) bool {
	switch inputType.(type) {
	case int, int64, float32, float64, string, bool, time.Time:
		return true
	default:
		return false
	}
}

//This funcion separates the fields with an 'AND' and starts the sentence with a 'WHERE'
//Eg. return value: "WHERE name = 'John' AND surname = 'Lennon'"
//Mostly used as a filter
func printAllFieldWithValue(fields []FieldWithValue, fieldTable ...string) string {
	if len(fields) == 0 {
		return ""
	}
	table := ""
	if len(fieldTable) == 1 {
		table = fieldTable[0]
	}
	retString := "WHERE "
	for i, f := range fields {
		if i != len(fields)-1 {
			retString += f.print(table) + " AND "
		} else {
			retString += f.print(table)
		}
	}
	return retString
}

//This funcion separates the fields with a ', '
//Eg. return value: "name = 'John', surname = 'Lennon'
//Mostly used on the update query to inform about the new attributes
func printAllNewFieldWithValue(fields []FieldWithValue) string {
	retString := ""
	for i, f := range fields {
		if i != len(fields)-1 {
			retString += f.print("") + ", "
		} else {
			retString += f.print("")
		}
	}
	return retString
}

//FieldName is the structure used to generate a field with just a name
//Eg.: FieldName{"name"		}
//Eg.: FieldName{"age"		}
//Eg.: FieldName{"is_adult"	}
//This struct makes generating the SQL query easier and therefore is simple but necessary
type FieldName string

func (fn *FieldName) print() string {
	return string(*fn)
}

func printAllFieldsName(fields []FieldName) string {
	if len(fields) == 0 {
		return "*"
	}
	retString := ""
	for i, f := range fields {
		if i != len(fields)-1 {
			retString += f.print() + ", "
		} else {
			retString += f.print()
		}
	}
	return retString
}

//FieldWithSorting is the structure used to generate a field with an associated sorting order.
//Eg.: FieldWithSorting	{FieldName:"name",		IsAscending:false	}
//Eg.: FieldWithSorting	{FieldName:"age",		IsAscending:true	}
//Eg.: FieldWithSorting	{FieldName:"is_adult",	IsAscending:false	}
//This struct makes generating the SQL query easier and therefore is simple but necessary
type FieldWithSorting struct {
	FieldName   string
	IsAscending bool
}

func (fws *FieldWithSorting) print() string {
	if fws.IsAscending {
		return fmt.Sprintf("%s ASC", fws.FieldName)
	}
	return fmt.Sprintf("%s DESC", fws.FieldName)
}

func printAllFieldWithSorting(fields []FieldWithSorting) string {
	if len(fields) == 0 {
		return ""
	}
	retString := "ORDER BY "
	for i, f := range fields {
		if i != len(fields)-1 {
			retString += f.print() + ", "
		} else {
			retString += f.print()
		}
	}
	return retString
}

//JoinField is the structure used to generate a SQL join between two tables.
//Eg.: JoinField	{JoinFromTable:"users",	JoinFromAttribute:"user_reviews", JoinToTable:"reviews", JoinToAttribute:"id"}
//This struct makes generating the SQL query easier and therefore is simple but necessary
type JoinField struct {
	JoinFromTable     string
	JoinFromAttribute string
	JoinToTable       string
	JoinToAttribute   string
}

func (jf *JoinField) print() string {
	return fmt.Sprintf("JOIN %s ON %s.%s = %s.%s", jf.JoinToTable,
		jf.JoinFromTable, jf.JoinFromAttribute, jf.JoinToTable, jf.JoinToAttribute)
}

func printAllJoinFields(fields []JoinField) string {
	retString := ""
	for i, f := range fields {
		if i != len(fields)-1 {
			retString += f.print() + " "
		} else {
			retString += f.print()
		}
	}
	return retString
}

//MakeDeleteQuery generates a delete query on SQL.
//It receives the table name and the filter values for selecting which row to deletet
//Eg.: MakeDeleteQuery("barcelona_players", FieldWithValue{"name","Luis"}, FieldWithValue{"surname","Suarez"})
//Eg. response: "DELETE FROM barcelona_players WHERE name = 'Luis' AND surname = 'Suarez';"
func MakeDeleteQuery(tableName string, filter ...FieldWithValue) string {
	return fmt.Sprintf("DELETE FROM %s %s;", tableName, printAllFieldWithValue(filter))
}

//MakeSelectQuery generates a select query on SQL.
//It receives the fields we want to get (If nil or empty, then every field will be returned)
//It also reveives the tableName, filterFields (can be null), and SortingFields (can be null).
//Lastly, it receives the JoinFields for joining other tables (can be null too).
//Example query:
// selectionFields 	:= []FieldName			{"name", "surname", "golden_boot_year"}
// tableName 		:= "players"
// filterFields 	:= []FieldWithValue		{{FieldName:"height",	FieldValue:"1.70"}}
// sortingFields	:= []FieldWithSorting	{{FieldName:"name",		IsAscending:true}}
// joinFields		:= JoinField			{JoinFromTable:"players",	JoinFromAttribute:"id",
//											 JoinToTable:"golden_boots", JoinToAttribute:"player_who_won_it_id"}
// MakeSelectQuery(selectionFields, tableName, filterFields, sortingFields, joinFields)
//Eg. response: "SELECT name, surname, golden_boot_year FROM players JOIN golden_boots ON players.id = golden_boots.player_who_won_it_id WHERE height = 1.70 ORDER BY name ASC;"
func MakeSelectQuery(selectionFields []FieldName, tableName string, filterFields []FieldWithValue, sortingFields []FieldWithSorting, joinFields ...JoinField) string {
	return fmt.Sprintf("SELECT %s FROM %s %s %s %s;",
		printAllFieldsName(selectionFields), tableName, printAllJoinFields(joinFields),
		printAllFieldWithValue(filterFields, tableName), printAllFieldWithSorting(sortingFields))
}

//MakeUpdateQuery generates an update query on SQL.
//It receives the table name, the new values that updated fields, and an opcional filter for selecting which row to update
//Eg.: MakeUpdateQuery("all_time_scorers", []FieldWithValue{{"name","Cristiano"},{"surname", "Ronaldo"}}, []FieldWithValue{{"name","Pele"}})
//Eg. response: UPDATE all_time_scorers SET name = 'Cristiano', surname = 'Ronaldo' WHERE name = 'Pele';
func MakeUpdateQuery(tableName string, newValues []FieldWithValue, filter []FieldWithValue, joinFields ...JoinField) string {
	return fmt.Sprintf("UPDATE %s SET %s %s %s;",
		tableName, printAllNewFieldWithValue(newValues), printAllFieldWithValue(filter), printAllJoinFields(joinFields))
}

//MakeInsertQuery generates an insert query on SQL.
//It receives the table name and the new values that the fields on the talbe will have
//Eg.: MakeInsertQuery("goats", FieldWithValue{"name","Kylian"}, FieldWithValue{"surname","Mbappe"})
//Eg. response: "INSERT INTO goats (name, surname) VALUES ('Kylian', 'Mbappe');"
func MakeInsertQuery(tableName string, newValues []FieldWithValue) string {
	fieldsName, fieldsValue := splitFieldsAndValues(newValues)

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		tableName, fieldsName, fieldsValue)
}

func splitFieldsAndValues(fusedValues []FieldWithValue) (string, string) {
	fieldsName := ""
	fieldsValue := ""
	for i, actual := range fusedValues {
		val := actual.printValue()
		if val != "" {
			if i != 0 && fieldsName != "" {
				fieldsName += ", "
				fieldsValue += ", "
			}

			fieldsName += actual.FieldName
			fieldsValue += val
		}
	}
	return fieldsName, fieldsValue
}

//MakeInsertQueryWithStruct generates an insert query on SQL.
//It receives the table name and the struct we will be mapping
//It only returns fields that are of type int, int64, float32, float64, string, bool and time.Time.
//Eg.: kobe := BasketPlayers{Name: "Kobe", Surname: "Bryant", Number: 8, PointsPerGame: 33.643, HasRetired: true,
// 				BirthDate: time.Date(1978, 8, 23, 0, 0, 0, 0, time.Now().Location()), Nicknames: []string{"Black Mamba", "KB24", "Little Fliying Warrior"}}
//Eg.: MakeInsertQueryWithStruct("goats", kobe)
//Eg. response: "INSERT INTO legends (surname, number, points_per_game, has_retired, birth_date, name) VALUES ('Bryant', 8, 33.643, true, '1978-08-23 00:00:00', 'Kobe');"
//Here we can see the formatting applied to the dateTime fields and how the []string was left out of the query.
func MakeInsertQueryWithStruct(tableName string, str interface{}) string {
	structTuples := breakDownStruct(str)
	fields := []FieldWithValue{}
	for _, tuple := range structTuples {
		fields = append(fields, FieldWithValue{tuple.Name, tuple.Value})
	}
	return MakeInsertQuery(tableName, fields)
}

//MergeManyInsertsIntoOneInsert merges many insert queries onto a single query of MySql.
//It receives the insert queries and returns just one
//Eg.:
//	query1 := "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Black Mamba');"
//	query2 := "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'KB24');"
//	query3 := "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Little Fliying Warrior');"
//It returns: "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Black Mamba') (1, 'KB24') (1, 'Little Fliying Warrior');"
func MergeManyInsertsIntoOneInsert(queries ...string) string {
	query := strings.Split(queries[0], ";")[0]
	for i := 1; i < len(queries); i++ {
		queryAttributes := strings.Split(queries[i], "VALUES")[1]
		query += strings.Split(queryAttributes, ";")[0]
	}
	return query + ";"
}

//Tuple is an internal struct used to store two values in a single structure
type Tuple struct {
	Name  string
	Value interface{}
}

func breakDownStruct(st interface{}) []Tuple {
	reqRules := []Tuple{}
	v := reflect.ValueOf(st)
	t := reflect.TypeOf(st)
	for i := 0; i < v.NumField(); i++ {
		key := strings.ToLower(t.Field(i).Name)
		typ := v.FieldByName(t.Field(i).Name).Kind().String()
		structTag := t.Field(i).Tag.Get("json")
		if structTag == "" {
			structTag = t.Field(i).Tag.Get("db")
		}
		jsonName := strings.TrimSpace(strings.Split(structTag, ",")[0])
		value := v.FieldByName(t.Field(i).Name)
		// if jsonName is not empty use it for the key
		if jsonName != "" && jsonName != "-" {
			key = jsonName
		}
		if typ == "string" {
			if !(value.String() == "" && strings.Contains(structTag, "omitempty")) {
				reqRules = append(reqRules, Tuple{Name: key, Value: value.String()})
			}
		} else if typ == "int" {
			reqRules = append(reqRules, Tuple{Name: key, Value: value.Int()})
		} else {
			reqRules = append(reqRules, Tuple{Name: key, Value: value.Interface()})
		}
	}
	return reqRules
}
