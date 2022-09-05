package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
	"log"
	"strconv"
)

type Category struct {
	Id          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Revision struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	OwnerId   int    `json:"owner_id"`
	CreatedAt string `json:"created_at"`
}

type Card struct {
	Id   int    `json:"id"`
	Text string `json:"text"`
}

type CategoryWithCards struct {
	Id          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Cards       []Card `json:"cards"`
}

type SessionResult struct {
	Id          int `json:"id"`
	AccessLevel int `json:"access_level"`
}

type ConnectionData struct {
	User     string
	Password string
}

var watcherConnection = getConnection(ConnectionData{
	User:     "watcher_user",
	Password: "8B137DEC7A74463EB1836CA141BEADB3",
})
var editorConnection = getConnection(ConnectionData{
	User:     "editor_user",
	Password: "FBEC626988EE4A02949A95C8B5BB113A",
})

func getConnection(data ConnectionData) *sql.DB {
	psqlInfo := fmt.Sprintf(
		"host=db port=5432 user=%s password=%s dbname=postgres sslmode=disable",
		data.User, data.Password)
	conn, err := sql.Open("postgres", psqlInfo)
	if err != nil || conn == nil {
		panic(errors.New(fmt.Sprintf("Cannot connect to database with user %s", data.User)))
	}
	return conn
}

func main() {
	app := fiber.New()
	app.Get("/check", func(c *fiber.Ctx) error {
		return checkHandler(c)
	})
	app.Get("/categories", func(c *fiber.Ctx) error {
		return getCategoriesHandler(c)
	})
	app.Get("/cards", func(c *fiber.Ctx) error {
		return getCardsHandler(c)
	})
	app.Get("/version", func(c *fiber.Ctx) error {
		return versionHandler(c)
	})
	app.Post("/auth", func(c *fiber.Ctx) error {
		return authHandler(c)
	})
	app.Post("/session", func(c *fiber.Ctx) error {
		return sessionHandler(c)
	})
	app.Get("/revision/list", func(c *fiber.Ctx) error {
		return revisionListHandler(c)
	})
	app.Post("/revision/new", func(c *fiber.Ctx) error {
		return revisionCreateHandler(c)
	})
	app.Post("/revision/remove", func(c *fiber.Ctx) error {
		return revisionRemoveHandler(c)
	})
	app.Post("/revision/category/new", func(c *fiber.Ctx) error {
		return revisionNewCategoryHandler(c)
	})
	app.Post("/revision/category/edit", func(c *fiber.Ctx) error {
		return revisionEditCategoryHandler(c)
	})
	app.Post("/revision/category/remove", func(c *fiber.Ctx) error {
		return revisionRemoveCategoryHandler(c)
	})
	app.Post("/revision/card/new", func(c *fiber.Ctx) error {
		return revisionNewCardHandler(c)
	})
	app.Post("/revision/card/edit", func(c *fiber.Ctx) error {
		return revisionEditCardHandler(c)
	})
	app.Post("/revision/card/remove", func(c *fiber.Ctx) error {
		return revisionRemoveCardHandler(c)
	})
	app.Get("/revision/apply", func(c *fiber.Ctx) error {
		return revisionApplyHandler(c)
	})
	app.Post("/register", func(c *fiber.Ctx) error {
		return revisionRegisterHandler(c)
	})
	err := app.Listen(":3000")
	if err != nil {
		return
	}
}

func revisionRegisterHandler(ctx *fiber.Ctx) error {
	body := struct {
		Name        string `json:"name"`
		Login       string `json:"login"`
		Password    string `json:"password"`
		AccessLevel int    `json:"access_level"`
	}{}
	if err := ctx.BodyParser(&body); err != nil || body.Name == "" || body.Login == "" || body.Password == "" {
		return fiber.NewError(400, "Please, specify 'name', 'login', 'password', 'access_level' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	if session.AccessLevel < 2 {
		return fiber.ErrForbidden
	}

	rows, err := editorConnection.Query("select cw.register($1, $2, $3, $4::smallint);",
		body.Name, body.Login, body.Password, body.AccessLevel)

	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	if id == -1 {
		return fiber.NewError(400, "User already exists")
	}

	return ctx.SendString(strconv.Itoa(id))
}

func revisionApplyHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int `json:"revision_id"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	_, err = editorConnection.Exec("call cw.revision_apply($1);", body.RevisionId)
	if err != nil {
		return databaseError(err)
	}
	return ctx.SendStatus(200)
}

func revisionRemoveHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int `json:"revision_id"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	_, err = editorConnection.Exec("call cw.remove_revision($1);", body.RevisionId)
	if err != nil {
		return databaseError(err)
	}
	return ctx.SendStatus(200)
}

func revisionRemoveCardHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int `json:"revision_id"`
		CategoryId int `json:"category_id"`
		CardId     int `json:"card_id"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id', 'category_id' and 'card_id' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}
	rows, err = editorConnection.Query("SELECT cw.remove_card_edition($1, $2, $3)",
		body.RevisionId, body.CategoryId, body.CardId)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var removed bool
	err = rows.Scan(&removed)
	if err != nil {
		return databaseError(err)
	}

	if !removed {
		return fiber.NewError(400, "Card not exists")
	}

	return ctx.SendStatus(200)
}

func revisionEditCardHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int    `json:"revision_id"`
		CategoryId int    `json:"category_id"`
		CardId     int    `json:"card_id"`
		Text       string `json:"text"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id', 'card_id' and optional 'text' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	if body.Text == "" {
		rows, err = editorConnection.Query("SELECT cw.edit_card($1, $2, $3)",
			body.RevisionId, body.CategoryId, body.CardId)
	} else {
		rows, err = editorConnection.Query("SELECT cw.edit_card($1, $2, $3, $4)",
			body.RevisionId, body.CategoryId, body.CardId, body.Text)
	}

	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	if id == -1 {
		return fiber.NewError(400, "Card is already edits on another revision")
	} else if id == -2 {
		return fiber.NewError(400, "Category not exists")
	}

	return ctx.JSON(fiber.Map{"id": id})
}

func revisionNewCardHandler(ctx *fiber.Ctx) error {

	body := struct {
		RevisionId int    `json:"revision_id"`
		CategoryId int    `json:"category_id"`
		Text       string `json:"text"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id', 'category_id' 'text' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	rows, err = editorConnection.Query("SELECT cw.add_card($1, $2, $3)",
		body.RevisionId, body.CategoryId, body.Text)

	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	if id == -1 {
		return fiber.NewError(400, "Category not exists")
	} else if id == -2 {
		return fiber.NewError(400, "Card with this name are already exists in this category")
	}

	return ctx.JSON(fiber.Map{"id": id})

}

func revisionRemoveCategoryHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int `json:"revision_id"`
		CategoryId int `json:"category_id"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id' and 'category_id' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}
	rows, err = editorConnection.Query("SELECT cw.remove_category_edition($1, $2)",
		body.RevisionId, body.CategoryId)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var removed bool
	err = rows.Scan(&removed)
	if err != nil {
		return databaseError(err)
	}

	if !removed {
		return fiber.NewError(400, "Category not exists")
	}

	return ctx.SendStatus(200)
}

func revisionEditCategoryHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId  int    `json:"revision_id"`
		CategoryId  int    `json:"category_id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id', 'category_id' and optional 'name', 'description' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	if body.Name != "" && body.Description != "" {
		rows, err = editorConnection.Query("SELECT cw.edit_category($1, $2, $3, $4)",
			body.RevisionId, body.CategoryId, body.Name, body.Description)
	} else if body.Name != "" {
		rows, err = editorConnection.Query("SELECT cw.edit_category(revision_id_in := $1, category_id_in := $2, name := $3)",
			body.RevisionId, body.CategoryId, body.Name)
	} else if body.Description != "" {
		rows, err = editorConnection.Query("SELECT cw.edit_category(revision_id_in := $1, category_id_in := $2, description := $3)",
			body.RevisionId, body.CategoryId, body.Description)
	} else {
		rows, err = editorConnection.Query("SELECT cw.edit_category(revision_id_in := $1, category_id_in := $2)",
			body.RevisionId, body.CategoryId)
	}
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	if id == -1 {
		return fiber.NewError(400, "Category name are already taken")
	} else if id == -2 {
		return fiber.NewError(400, "Null fields for non-exist category are not allown")
	}

	return ctx.JSON(fiber.Map{"id": id})
}

func revisionNewCategoryHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId  int    `json:"revision_id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'revision_id', 'name', 'description' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, body.RevisionId)
		if err != nil {
			return err
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}
	rows, err = editorConnection.Query("SELECT cw.add_category($1, $2, $3)",
		body.RevisionId, body.Name, body.Description)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	if id == -1 {
		return fiber.NewError(400, "Duplicate name of category")
	}

	return ctx.JSON(fiber.Map{"id": id})
}

// please make sure user is authorized
func owns(userId int, revisionId int) (bool, error) {
	rows, err := editorConnection.Query("SELECT cw.owns($1, $2)", userId, revisionId)
	if err != nil {
		return false, databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return false, databaseError(err)
	}
	var owns bool
	err = rows.Scan(&owns)
	if err != nil {
		return false, databaseError(err)
	}
	return owns, nil
}

func revisionListHandler(ctx *fiber.Ctx) error {
	body := struct {
		RevisionId int `json:"revision_id"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
	}

	if body.RevisionId != 0 {
		return revisionCategoriesListHandler(ctx, body.RevisionId)
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		rows, err = editorConnection.Query(
			"SELECT r.id, r.name, r.owner_id, r.created_at FROM cw.revision r WHERE owner_id = $1",
			session.Id,
		)
		if err != nil {
			return databaseError(err)
		}
		defer closeRows(rows)
	} else {
		rows, err = editorConnection.Query("SELECT r.id, r.name, r.owner_id, r.created_at FROM cw.revision r")
		if err != nil {
			return databaseError(err)
		}
		defer closeRows(rows)
	}
	var revisions = make([]Revision, 0)
	for rows.Next() {
		var row Revision
		err := rows.Scan(&row.Id, &row.Name, &row.OwnerId, &row.CreatedAt)
		if err != nil {
			return databaseError(err)
		}
		revisions = append(revisions, row)
	}
	return ctx.JSON(fiber.Map{"revisions": revisions})
}

func revisionCategoriesListHandler(ctx *fiber.Ctx, revisionId int) error {
	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	} else if session.AccessLevel == 1 {
		owns, err := owns(session.Id, revisionId)
		if err != nil {
			return databaseError(err)
		}
		if !owns {
			return fiber.ErrForbidden
		}
	}

	rows, err = editorConnection.Query("select * from cw.revision_json($1)", revisionId)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return databaseError(errors.New("empty function output"))
	}
	var resultJson string
	err = rows.Scan(&resultJson)
	if err != nil {
		return databaseError(err)
	}

	ctx.Set("Content-type", "application/json; charset=utf-8")
	return ctx.SendString(resultJson)
}

func revisionCreateHandler(ctx *fiber.Ctx) error {
	body := struct {
		Name string `json:"name"`
	}{}
	if err := ctx.BodyParser(&body); err != nil {
		return fiber.NewError(400, "Please, specify 'name' via body")
	}

	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if session.AccessLevel < 1 {
		return fiber.ErrForbidden
	}

	rows, err = editorConnection.Query(
		"INSERT INTO cw.revision (name, owner_id, created_at) VALUES ($1, $2, now()) RETURNING id",
		body.Name, session.Id)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return fiber.NewError(500, "Unexpected, cannot create")
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		return databaseError(err)
	}

	return ctx.JSON(fiber.Map{"id": id})
}

func closeRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.Println("Close rows error: " + err.Error())
	}
}

func versionHandler(ctx *fiber.Ctx) error {
	var version string
	rows, err := watcherConnection.Query("SELECT cw.v();")
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	rows.Next()
	err = rows.Scan(&version)
	if err != nil {
		return databaseError(err)
	}
	return ctx.SendString(version)
}

func checkHandler(ctx *fiber.Ctx) error {
	return ctx.SendString("check1")
}

func getCategoriesHandler(ctx *fiber.Ctx) error {
	var categories = make([]Category, 0)
	rows, err := watcherConnection.Query("SELECT c.id, c.name, c.description FROM cw.Category c;")
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	for rows.Next() {
		var row Category
		err := rows.Scan(&row.Id, &row.Name, &row.Description)
		if err != nil {
			return databaseError(err)
		}
		categories = append(categories, row)
	}
	return ctx.JSON(fiber.Map{"categories": categories})
}

func getCardsHandler(ctx *fiber.Ctx) error {
	var cards = make([]Card, 0)
	var category Category
	body := struct {
		CategoryId int `json:"category"`
	}{}
	if err := ctx.BodyParser(&body); err != nil || body.CategoryId < 1 {
		return fiber.NewError(400, "Please, specify 'category' via body")
	}
	rows, err := watcherConnection.Query("SELECT name, description FROM cw.category WHERE id=$1;", body.CategoryId)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if rows.Next() {
		err := rows.Scan(&category.Name, &category.Description)
		if err != nil {
			return databaseError(err)
		}
	} else {
		return fiber.NewError(400, "Category not exist")
	}
	rows, err = watcherConnection.Query("SELECT c.id, c.text FROM cw.card c WHERE category_id=$1;", body.CategoryId)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	for rows.Next() {
		var row Card
		err := rows.Scan(&row.Id, &row.Text)
		if err != nil {
			return databaseError(err)
		}
		cards = append(cards, row)
	}
	if len(cards) == 0 {
		return fiber.NewError(400, "There are no cards with this category")
	}
	categoryWithCards := CategoryWithCards{Id: body.CategoryId, Name: category.Name, Description: category.Description, Cards: cards}
	return ctx.JSON(categoryWithCards)
}

func authHandler(ctx *fiber.Ctx) error {
	cred := struct {
		Login    string `json:"login"`
		Password string `json:"password"`
	}{}
	if err := ctx.BodyParser(&cred); err != nil || cred.Login == "" || cred.Password == "" {
		return fiber.NewError(400, "Please, specify 'login' and 'password' via body")
	}

	rows, err := watcherConnection.Query(
		"SELECT a.id, a.token, a.access_level FROM cw.auth($1, $2) as a",
		cred.Login, cred.Password,
	)
	if err != nil {
		return databaseError(err)
	}
	defer closeRows(rows)
	if !rows.Next() {
		return fiber.ErrUnauthorized
	}

	authResult := struct {
		Id          int    `json:"id"`
		Token       string `json:"token"`
		AccessLevel int    `json:"access_level"`
	}{}
	err = rows.Scan(&authResult.Id, &authResult.Token, &authResult.AccessLevel)
	if err != nil {
		panic(err)
	}
	if authResult.Id == -1 {
		return fiber.ErrNotFound
	}
	return ctx.JSON(authResult)
}

func sessionHandler(ctx *fiber.Ctx) error {
	session, err := authorizeRequest(ctx)
	if err != nil {
		return err
	}
	return ctx.JSON(*session)
}

func authorizeRequest(ctx *fiber.Ctx) (*SessionResult, error) {
	token := ctx.GetReqHeaders()["Session"]
	if len(token) != 32 {
		if len(token) == 0 {
			return nil, fiber.ErrUnauthorized
		} else {
			return nil, fiber.NewError(400, "Invalid token")
		}
	}
	session, err := checkAuth(token)
	if err != nil {
		return nil, fiber.ErrUnauthorized
	}
	return &session, nil
}

func checkAuth(token string) (SessionResult, error) {
	rows, err := watcherConnection.Query(
		"SELECT s.id, s.access_level FROM cw.session($1) as s",
		token,
	)
	if err != nil {
		return SessionResult{}, err
	}
	defer closeRows(rows)
	if !rows.Next() {
		return SessionResult{}, err
	}
	var sessionResult SessionResult
	err = rows.Scan(&sessionResult.Id, &sessionResult.AccessLevel)
	if err != nil || sessionResult.Id == -1 || sessionResult.AccessLevel == -1 {
		return SessionResult{}, errors.New("user not exists")
	}
	return sessionResult, err
}

func databaseError(err error) *fiber.Error {
	log.Println(err)
	return fiber.NewError(423, "Database error")
}
