package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"time"
)

var adminConnection = getConnection()

func databaseError(err error) error {
	log.Println(err)
	return err
}

func getConnection() *sql.DB {
	psqlInfo := fmt.Sprintf(
		"port=5432 user=postgres password=postgres dbname=postgres sslmode=disable host=localhost")
	conn, err := sql.Open("postgres", psqlInfo)
	if err != nil || conn == nil {
		panic(err)
	}
	return conn
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	removeData()

	for i := 2; i < 1000; i = i * 2 {
		times := 1
		cardsForEachCategory := i * 3
		goTime, sqlTime := checkPerformance(times, i, cardsForEachCategory)
		fmt.Printf("Parsed %d categories and %d cards for each: SQL - %.2fms, Go - %.2fms (avg for %d times)\n", i, cardsForEachCategory, sqlTime/float32(times), goTime/float32(times), times)
	}
}

func checkPerformance(times int, categoriesCount int, cardsPerCategory int) (float32, float32) {
	insertData(categoriesCount, cardsPerCategory)

	goStart := time.Now()
	for i := 0; i < times; i++ {
		err, _ := revisionCategoriesListByCode()
		if err != nil {
			panic(err)
		}
	}
	goTime := time.Since(goStart).Milliseconds()

	sqlStart := time.Now()
	for i := 0; i < times; i++ {
		err, _ := revisionCategoriesListBySQL()
		if err != nil {
			panic(err)
		}
	}
	sqlTime := time.Since(sqlStart).Milliseconds()

	removeData()
	return float32(goTime), float32(sqlTime)
}

func removeData() {
	adminConnection.Exec("call cw.remove_revision(-1);")
}

func insertData(categoriesCount int, cardsPerCategory int) {
	adminConnection.Exec("INSERT INTO cw.user (id, name, login, password, access_level) VALUES (-1, 'Experimental', 'exp', 'experiment', 3) on conflict do nothing")
	adminConnection.Exec("INSERT INTO cw.revision (id, name, owner_id, created_at) VALUES (-1, 'exp', -1, now())")
	revId := -1
	for i := 0; i < categoriesCount; i++ {
		catId := -i
		catCatId := -i
		newName := randString(10)
		newDesc := randString(20)

		adminConnection.Exec("INSERT INTO cw.categoryt (id, revision_id, category_id, new_name, new_description) VALUES ($1, $2, $3, $4, $5)",
			catId, revId, catCatId, newName, newDesc)

		valuesString := ""
		for j := 0; j < cardsPerCategory; j++ {
			cId := -(i*cardsPerCategory + j)
			newText := randString(15)
			if j != 0 {
				valuesString = valuesString + ","
			}
			valuesString = valuesString + fmt.Sprintf("(%d, %d, %d, %d, '%s')", cId, revId, catCatId, cId, newText)
		}
		adminConnection.Exec(fmt.Sprintf("INSERT INTO cw.cardt (id, revision_id, category_id, card_id, new_text) VALUES %s", valuesString))
	}

}

func closeRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.Println("Close rows error: " + err.Error())
	}
}

func revisionCategoriesListBySQL() (error, string) {
	rows, err := adminConnection.Query("select * from cw.revision_json($1)", -1)
	if err != nil {
		return databaseError(err), ""
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(errors.New("empty function output")), ""
	}
	var resultJson string
	err = rows.Scan(&resultJson)
	if err != nil {
		return databaseError(err), ""
	}
	return nil, resultJson
}

func revisionCategoriesListByCode() (error, string) {
	var revisionId = -1
	rows, err := adminConnection.Query("select c.category_id cat_id, cat.new_name cat_name, cat.new_description cat_desc, c.card_id c_id, c.new_text c_text from cw.categoryt cat join cw.cardt c on cat.category_id = c.category_id where c.revision_id = $1 order by c.category_id", revisionId)
	defer closeRows(rows)

	type CategoryWithCards struct {
		CatID          int    `json:"cat_id"`
		CatName        string `json:"cat_name"`
		CatDescription string `json:"cat_desc"`
		CardID         int    `json:"c_id"`
		CardText       string `json:"c_text"`
	}

	type Card struct {
		CardId   int    `json:"card_id"`
		CardText string `json:"card_text"`
	}

	type CategoryWithCardsList struct {
		CatID          int    `json:"category_id"`
		CatName        string `json:"category_name"`
		CatDescription string `json:"category_desc"`
		Cards          []Card `json:"cards"`
	}

	var categories = make([]CategoryWithCards, 0)
	if err != nil {
		return databaseError(err), ""
	}
	for rows.Next() {
		var row CategoryWithCards
		err := rows.Scan(&row.CatID, &row.CatName, &row.CatDescription, &row.CardID, &row.CardText)
		if err != nil {
			return databaseError(err), ""
		}
		categories = append(categories, row)
	}

	currentCategoryId := 0
	categoriesWithList := make([]CategoryWithCardsList, 0)
	cards := make([]Card, 0)
	for i, s := range categories {
		if i == 0 {
			currentCategoryId = s.CatID
		}
		if currentCategoryId != s.CatID || i+1 == len(categories) {
			var cat CategoryWithCardsList
			cat.CatID = s.CatID
			cat.CatName = s.CatName
			cat.CatDescription = s.CatDescription
			cat.Cards = cards
			categoriesWithList = append(categoriesWithList, cat)
			cards = make([]Card, 0)
			currentCategoryId = s.CatID
		}
		var card Card
		card.CardId = s.CardID
		card.CardText = s.CardText
		cards = append(cards, card)
	}

	rows, err = adminConnection.Query("select name, created_at from cw.revision where id = $1", revisionId)
	type RevisionWithCategories struct {
		Name       string                  `json:"name"`
		CreatedAt  string                  `json:"created_at"`
		Categories []CategoryWithCardsList `json:"categories"`
	}
	var revision RevisionWithCategories
	if !rows.Next() {
		return databaseError(errors.New("empty function output")), ""
	}
	err = rows.Scan(&revision.Name, &revision.CreatedAt)
	if err != nil {
		return databaseError(err), ""
	}
	revision.Categories = categoriesWithList
	result, err := json.Marshal(revision)
	if err != nil {
		return err, ""
	}
	return nil, string(result)
}
