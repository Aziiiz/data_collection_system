package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
)

// product to collect data struct
type Menu struct {
	Menu_id  int    // menu id
	Title    string // title of the product to collect
	Url_path string // path of product
}

// collected product data struct
type Product struct {
	Id      int    // id of the product
	Title   string // title of the product
	ImgPath string // img path of the title
}

// status for menu data struct
type Status struct {
	Menu_id int // id of the menu
	Status  int // status of the menu 0 == default active, 1 == collected , 2 == not active
}

type ElData struct {
	Id    int    `json:"id"`
	Title string `json:"title"`
}

// create mysql connection
func dbConnect() (db *sql.DB) {

	dbDriver := "mysql"         // database name
	dbUser := "root"            // user name
	dbPass := "1234"            // password
	dbName := "data_collection" // schema name

	// connection to db
	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)

	// check connection for errors
	if err != nil {
		fmt.Println("error occured while connecting to db", err)
		return db
	}
	//fmt.Println("db is connected succesfully")
	return db

}

// select all menu data from tb_menu table
func selectMenu() []Menu {

	list := Menu{}                                                                                                            // make list variable
	res := []Menu{}                                                                                                           // set response value
	db := dbConnect()                                                                                                         // connect to db
	menuDB, err := db.Query("SELECT  tb_menu.menu_id, tb_menu.title, tb_menu.url_path FROM tb_menu WHERE tb_menu.status = 0") // make a query to db
	if err != nil {
		//panic(err.Error())
		fmt.Println("error making query to database ", err)
		defer db.Close() // close database
		return res       // return response
	}

	// loop db result
	for menuDB.Next() {
		var menu_id int     // set variable for menu id
		var title string    // set variable for title
		var url_path string // set variable for url_path

		// check each value
		err = menuDB.Scan(&menu_id, &title, &url_path)
		if err != nil {
			//panic(err.Error())
			fmt.Println("error occured while checking reading data from database ", err)
			defer db.Close()
			return res
		}
		// set db result data
		list.Menu_id = menu_id   // set for menu id
		list.Title = title       // set for title
		list.Url_path = url_path // set for url_path

		// push data to res array
		res = append(res, list)
	}
	defer db.Close() // close db
	return res       // return res array data
}

// update menu table status
func updateMenu(status []Status) bool {

	res := false                                             // set res false as  default
	db := dbConnect()                                        // connect to db
	updateSql := "UPDATE tb_menu SET tb_menu.status =( case" // set query statement
	condition := " WHERE tb_menu.menu_id IN ("
	caseList := []interface{}{}  //interface array for case clouse
	whereList := []interface{}{} // interface array for where clouse
	fmt.Println("list of menuLists ", len(status))
	//counter := 0
	for _, row := range status {
		//fmt.Println("menuData should be update ", row.Menu_id, "/", row.Status)
		caseList = append(caseList, row.Menu_id, row.Status)
		whereList = append(whereList, row.Menu_id)
		fmt.Println(caseList)
		updateSql += (" WHEN tb_menu.menu_id = ? THEN ? ")
		condition += "?,"

	}
	fmt.Println("list should be passed to db ", caseList)
	updateSql += "end)"
	condition = strings.TrimSuffix(condition, ",") // remove last "," from query statement
	condition += ") "
	updateSql += condition

	//updateSql = strings.TrimSuffix(updateSql, ",") // remove  last "," from query statement
	update, err := db.Prepare(updateSql)
	if err != nil {
		fmt.Println(" error occured on Preparedb function ", err)
		res = false
		defer db.Close()
		return res
	}
	set, err := update.Exec(append(caseList, whereList...)...) // query data to db
	if err != nil {
		fmt.Println("error occured while updating menu list ", err)
		res = false
		defer db.Close()
		return res
	}
	fmt.Println("menu list updated ", set)
	defer db.Close()
	res = true
	return res
}

// insert data to tb_product data table
func insertProducts(products []Product, total int) bool {
	var res bool
	res = false
	db := dbConnect()
	// insert sql statement
	insertSql := "INSERT INTO tb_product(title, img_url, date) VALUES"
	prods := []interface{}{}
	fmt.Println("list of products ", len(products))
	counter := 0
	for _, row := range products {
		insertSql += "(?,?, NOW()),"
		counter++
		prods = append(prods, row.Title, row.ImgPath)
	}
	fmt.Println("all data ", prods)
	fmt.Println("sql statement ", insertSql)
	insertSql = strings.TrimSuffix(insertSql, ",")
	insert, err := db.Prepare(insertSql)
	if err != nil {
		fmt.Println(" error occured on Preparedb function ", err)
		res = false
		defer db.Close()
		return res
	}
	set, err := insert.Exec(prods...)
	if err != nil {
		fmt.Println("error in Exec function ", err)
		res = false
		defer db.Close()
		return res
	}
	fmt.Println("for loop worked ", counter)
	fmt.Println("data inserted to be result ", set)
	defer db.Close()
	res = true
	return res
}

func selectProducts() bool {
	state := false
	list := Product{}                                                                 // make list variable
	res := []Product{}                                                                // set response value
	db := dbConnect()                                                                 // connect to db
	menuDB, err := db.Query("SELECT tb_product.id, tb_product.title FROM tb_product") // make a query to db
	if err != nil {
		//panic(err.Error())
		fmt.Println("error making query to database ", err)
		defer db.Close() // close database
		return state     // return response
	}
	var id int
	var title string

	// loop db result
	for menuDB.Next() {

		// check each value
		err = menuDB.Scan(&id, &title)
		if err != nil {
			//panic(err.Error())
			fmt.Println("error occured while checking reading data from database ", err)
			defer db.Close()

			return state
		}
		// set db result data
		list.Id = id       // set for  id
		list.Title = title // set for title

		// push data to res array
		res = append(res, list)
	}
	state = true
	defer db.Close() // close db
	SaveProdsToElasticsearch(res)
	return state
}

func SaveProdsToElasticsearch(products []Product) bool {

	res := false
	url := "http://localhost:9200/test1/_doc?pretty"
	fmt.Println("URL>> ", url)

	for _, row := range products {

		body := &ElData{
			Id:    row.Id,
			Title: row.Title}

		buf := new(bytes.Buffer)
		json.NewEncoder(buf).Encode(body)
		req, err := http.NewRequest("POST", url, buf)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {

			return res
		}
		defer resp.Body.Close()

		fmt.Println("response Status:", resp.Status)

	}
	res = true
	return res
}

// parse html
func parseHtml(url string) ([]Product, int) {

	output := Product{}
	counter := 0
	res := []Product{}
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	request, err := http.NewRequest("Get", url, nil) // request here "GET is http type" "url is path" "nil is 0"
	if err != nil {
		fmt.Println("error requesting [[", url, "]] url", err)
		return res, counter

	}

	response, err := client.Do(request)
	if err != nil {
		//	panic(err.Error())
		fmt.Println("error setting  [[", url, "]] url header", err)
		return res, counter
	}
	defer response.Body.Close() // close response.body
	//defer client.CloseIdleConnections()

	doc, err := goquery.NewDocumentFromReader(response.Body) //parse html goquery
	if err != nil {
		//panic(err.Error())
		fmt.Println("error getting body of the response", err)
		return res, counter
	}

	fmt.Println("body", response.Body)

	// analayze html as default from search-result id
	doc.Find("#search-results").Find(".s-item-container").Each(func(i int, s *goquery.Selection) {

		title := s.Find("h2").Text()
		img_path, exists := s.Find(".a-fixed-left-grid-col.a-col-left").Find("img").Attr("src")
		fmt.Println("set path", img_path, "/", title)
		if exists {
			fmt.Println("title: ", title, "\nimg path ", img_path)
			output.Title = title
			output.ImgPath = img_path
			fmt.Println(output.Title)
			fmt.Println(output.ImgPath)
			res = append(res, output)
			counter++
		}
	})
	if counter == 0 {
		// check if upper code did not work use below code
		fmt.Println("category changed design", counter)
		doc.Find(".sg-col-inner").Find(".s-include-content-margin.s-border-bottom").Each(func(i int, s *goquery.Selection) {

			title := s.Find("h2").Find(".a-size-medium.a-color-base.a-text-normal").Text()
			img_path, exists := s.Find(".s-image").Attr("src")
			fmt.Println("set path", img_path, "/", title)
			if exists {
				fmt.Println("title: ", title, "img path ", img_path)
				output.Title = title
				output.ImgPath = img_path
				fmt.Println(output.Title)
				fmt.Println(output.ImgPath)
				res = append(res, output)
				counter++
			}
		})
	}
	fmt.Println("collected data from   url[", url, "] ", counter)
	return res, counter
}

// url request and query
func htppUrlConnect() bool {

	status := false
	output := []Product{}
	menuStat := []Status{}
	list := selectMenu()
	total := 0
	if list != nil {
		start := time.Now()
		fmt.Println("time started ", start)
		for i := 0; i < len(list); i++ {
			fmt.Println("tb_menu list ", list[i])
			result, counter := parseHtml(list[i].Url_path)
			if counter != 0 {
				fmt.Println("data exists ", counter)
				output = append(output, result...)
				total = total + counter
				status := Status{list[i].Menu_id, 1}
				menuStat = append(menuStat, status)

			} else {
				fmt.Println("no data in[title: ", list[i].Title, "] [url:", list[i].Url_path, "] [", list[i].Menu_id, "]", counter)
				status := Status{list[i].Menu_id, 2}
				menuStat = append(menuStat, status)
			}

		}
		secStart := start.Unix()
		end := time.Now()
		secEnd := end.Unix()
		if total != 0 {
			duration := secEnd - secStart
			fmt.Println("data collection took ", duration, "seconds")
			fmt.Println("total ", total)
			insertProducts(output, total)
			updateMenu(menuStat)

			status = true
		} else {
			status = false
		}

	} else {
		fmt.Println("error menut data is empty ", list)
		status = false
	}

	return status

}

func main() {

	result := htppUrlConnect() // result is bool variable value is true if data collected false if data is not collected
	fmt.Println("data collected ", result)
	if result {
		selectProducts()
	}
}
