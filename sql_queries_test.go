package strikemysqlqueries

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestDeleteQueryBasic(t *testing.T) {
	query := MakeDeleteQuery("goats",
		FieldWithValue{FieldName: "name", FieldValue: "Leo"},
		FieldWithValue{FieldName: "surname", FieldValue: "Messi"})

	assert.EqualValues(t, "DELETE FROM goats WHERE name = 'Leo' AND surname = 'Messi';", query)
}

func TestSelectQueryBasic(t *testing.T) {
	selectionFields := []FieldName{"name, surname, golden_boots"}
	tableName := "players"
	filterFields := []FieldWithValue{{FieldName: "height", FieldValue: "1.64"}}
	sortingFields := []FieldWithSorting{{FieldName: "name", IsAscending: true}}
	joinFields := JoinField{JoinFromTable: "goats", JoinFromAttribute: "golden_boots", JoinToTable: "golden_boots", JoinToAttribute: "id"}

	query := MakeSelectQuery(selectionFields, tableName, filterFields, sortingFields, joinFields)
	assert.EqualValues(t, "SELECT name, surname, golden_boots FROM players JOIN golden_boots ON goats.golden_boots = golden_boots.id WHERE players.height = 1.64 ORDER BY name ASC;", query)
}

func TestUpdateQueryBasic(t *testing.T) {
	query := MakeUpdateQuery("all_time_scorers",
		[]FieldWithValue{
			{FieldName: "name", FieldValue: "Cristiano"},
			{FieldName: "surname", FieldValue: "Ronaldo"}},
		[]FieldWithValue{{FieldName: "name", FieldValue: "Pele"}})

	assert.EqualValues(t, "UPDATE all_time_scorers SET name = 'Cristiano', surname = 'Ronaldo' WHERE name = 'Pele';", query)
}

func TestUpdateQueryWithJoin(t *testing.T) {
	query := MakeUpdateQuery("people",
		[]FieldWithValue{
			{FieldName: "name", FieldValue: "Tomas"},
			{FieldName: "twitter_username", FieldValue: "newTomy"}},
		[]FieldWithValue{{FieldName: "id", FieldValue: "23"}},
		JoinField{JoinFromTable: "people", JoinToTable: "social_data", JoinFromAttribute: "id", JoinToAttribute: "user_id"})

	assert.EqualValues(t, "UPDATE people SET name = 'Tomas', twitter_username = 'newTomy' WHERE id = 23 JOIN social_data ON people.id = social_data.user_id;", query)
}

func TestInsertQueryBasic(t *testing.T) {
	query := MakeInsertQuery("goats",
		[]FieldWithValue{
			{FieldName: "name", FieldValue: "Leo"},
			{FieldName: "surname", FieldValue: "Messi"},
			{FieldName: "is_left", FieldValue: true},
			{FieldName: "height_in_meters", FieldValue: 1.64},
			{FieldName: "empty_strting", FieldValue: ""},
			{FieldName: "nilValue", FieldValue: nil}})

	assert.EqualValues(t, "INSERT INTO goats (name, surname, is_left, height_in_meters) VALUES ('Leo', 'Messi', true, 1.64);", query)
}

func TestInsertQueryWithBasicStruct(t *testing.T) {
	type BasketPlayers struct {
		ID            int       `db:"id"`
		Name          string    `db:"name"`
		Surname       string    `db:"surname"`
		Number        int       `db:"number"`
		PointsPerGame float32   `db:"points_per_game"`
		HasRetired    bool      `db:"has_retired"`
		BirthDate     time.Time `db:"birth_date"`
	}
	kobe := BasketPlayers{
		ID:            1,
		Name:          "Kobe",
		Surname:       "Bryant",
		Number:        8,
		PointsPerGame: 33.643,
		HasRetired:    true,
		BirthDate:     time.Date(1978, 8, 23, 0, 0, 0, 0, time.Now().Location()),
	}
	query := MakeInsertQueryWithStruct("basket_legends", kobe)

	assert.EqualValues(t, "INSERT INTO basket_legends (id, name, surname, number, points_per_game, has_retired, birth_date) VALUES (1, 'Kobe', 'Bryant', 8, 33.643, true, '1978-08-23 03:00:00');", query)
}

func TestInsertQueryWithStructWithList(t *testing.T) {
	type BasketPlayers struct {
		ID            int       `db:"id"`
		Name          string    `db:"name"`
		Surname       string    `db:"surname"`
		Number        int       `db:"number"`
		PointsPerGame float32   `db:"points_per_game"`
		HasRetired    bool      `db:"has_retired"`
		BirthDate     time.Time `db:"birth_date"`
		Nicknames     []string  `db:"nicknames"`
	}
	kobe := BasketPlayers{
		ID:            1,
		Name:          "Kobe",
		Surname:       "Bryant",
		Number:        8,
		PointsPerGame: 33.643,
		HasRetired:    true,
		BirthDate:     time.Date(1978, 8, 23, 0, 0, 0, 0, time.Now().Location()),
		Nicknames:     []string{"Black Mamba", "KB24", "Little Fliying Warrior"},
	}
	type NicknamesTable struct {
		ID       int    `db:"id"`
		Nickname string `db:"nickname"`
	}

	query := MakeInsertQueryWithStruct("basket_legends", kobe)

	insertNicknamesQueries := []string{}
	for _, nickname := range kobe.Nicknames {
		query := MakeInsertQueryWithStruct("basket_players_nicknames", NicknamesTable{ID: kobe.ID, Nickname: nickname})
		insertNicknamesQueries = append(insertNicknamesQueries, query)
	}
	insertNicknamesQueryJoined := MergeManyInsertsIntoOneInsert(insertNicknamesQueries...)

	assert.EqualValues(t, "INSERT INTO basket_legends (id, name, surname, number, points_per_game, has_retired, birth_date) VALUES (1, 'Kobe', 'Bryant', 8, 33.643, true, '1978-08-23 03:00:00');", query)
	assert.EqualValues(t, 3, len(insertNicknamesQueries))
	assert.EqualValues(t, "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Black Mamba');", insertNicknamesQueries[0])
	assert.EqualValues(t, "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'KB24');", insertNicknamesQueries[1])
	assert.EqualValues(t, "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Little Fliying Warrior');", insertNicknamesQueries[2])
	assert.EqualValues(t, "INSERT INTO basket_players_nicknames (id, nickname) VALUES (1, 'Black Mamba') (1, 'KB24') (1, 'Little Fliying Warrior');", insertNicknamesQueryJoined)
}
