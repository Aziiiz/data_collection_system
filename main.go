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
type menu struct {
	menuID  int    // menu id
	title   string // title of the product to collect
	URLPath string // path of product
}

// collected product data struct
type product struct {
	ID      int    // id of the product
	title   string // title of the product
	imgPath string // img path of the title
}

// status for menu data struct
type status struct {
	menuID int // id of the menu
	status int // status of the menu 0 == default active, 1 == collected , 2 == not active
}

//struct for elasticsearch
type elData struct {
	ID    int    `json:"id"`    // product id
	title string `json:"title"` // product title
}

// create mysql connection
func dbConnect() (db *sql.DB) {

	dbDriver := "mysql"         // database name
	dbUser := "user"            //            // user name
	dbPass := "password"        //           // password
	dbName := "data_collection" // schema name

	// connection to db
	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)

	// check connection for errors
	if nil != err {
		fmt.Println("error occured while connecting to db", err)
		return db
	}
	//fmt.Println("db is connected succesfully")
	return db

}

// select all menu data from tb_menu table
func selectMenu() []menu {

	// make list variable
	res := []menu{}                                                                                                        // set response value
	db := dbConnect()                                                                                                      // connect to db
	menuDB, err := db.Query("SELECT tb_menu.menu_id,tb_menu.title,tb_menu.url_path FROM tb_menu WHERE tb_menu.status = 0") // make a query to db
	if nil != err {
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
		if nil != err {
			//panic(err.Error())
			fmt.Println("error occured while checking reading data from database ", err)
			//defer db.Close()
			return res
		}

		// push data to res array
		res = append(res, menu{menuID: menu_id, URLPath: url_path})
	}
	defer db.Close() // close db
	return res       // return res array data
}

// update menu table status
func updateMenu(status []status) bool {

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
		caseList = append(caseList, row.menuID, row.status)
		whereList = append(whereList, row.menuID)
		fmt.Println(caseList)
		updateSql += (" WHEN tb_menu.menu_id = ? THEN ? ")
		condition += "?,"

	}
	fmt.Println("list should be passed to db ", caseList)
	updateSql += "end)"
	condition = strings.TrimSuffix(condition, ",") // remove last "," from query statement
	condition += ") "
	// add two strings
	updateSql += condition

	// prepare data to query to mysql
	update, err := db.Prepare(updateSql)
	if err != nil {
		fmt.Println(" error occured on Preparedb function ", err)
		res = false
		defer db.Close()
		return res
	}
	// update to mysql table
	set, err := update.Exec(append(caseList, whereList...)...) // query data to db
	if nil != err {
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
func insertProducts(products []product, total int) bool {
	var res bool
	res = false
	db := dbConnect()
	// insert sql statement
	insertSql := "INSERT INTO tb_product(title, img_url, date) VALUES"
	prods := []interface{}{}
	fmt.Println("list of products ", len(products))
	counter := 0
	// loop thourgh all products
	for _, row := range products {
		insertSql += "(?,?, NOW()),"
		counter++
		prods = append(prods, row.title, row.imgPath)
	}
	fmt.Println("all data ", prods)
	fmt.Println("sql statement ", insertSql)
	// remove last "," from instertSql
	insertSql = strings.TrimSuffix(insertSql, ",")
	// prepare query statement
	insert, err := db.Prepare(insertSql)
	if nil != err {
		fmt.Println(" error occured on Preparedb function ", err)
		res = false
		defer db.Close()
		return res
	}
	// insert products on one query
	set, err := insert.Exec(prods...)
	if nil != err {
		fmt.Println("error in Exec function ", err)
		res = false
		defer db.Close()
		return res
	}
	fmt.Println("for loop worked ", counter)
	fmt.Println("data inserted to be result ", set)
	// close db connection
	defer db.Close()
	res = true
	return res
}

// select all saved products
func selectProducts() bool {
	state := false
	list := product{}                                                                 // make list variable
	res := []product{}                                                                // set response value
	db := dbConnect()                                                                 // connect to db
	menuDB, err := db.Query("SELECT tb_product.id, tb_product.title FROM tb_product") // make a query to db
	if nil != err {
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
		if nil != err {
			//panic(err.Error())
			fmt.Println("error occured while checking reading data from database ", err)
			defer db.Close()

			return state
		}
		// set db result data
		list.ID = id       // set for  id
		list.title = title // set for title

		// push data to res array
		res = append(res, list)
	}
	state = true
	defer db.Close() // close db
	saveProdsToElasticsearch(res)
	return state
}

// save products to elasticsearch
func saveProdsToElasticsearch(products []product) bool {
	// set function to false
	res := false
	//elasticsearch url
	url := "http://localhost:9200/test1/_doc?pretty" // pretty used to make json data formatted
	fmt.Println("URL>> ", url)

	for _, row := range products {
		// set json data values
		body := &elData{
			ID:    row.ID,
			title: row.title}
		//set http post request
		buf := new(bytes.Buffer)
		// endoce json file to byte
		json.NewEncoder(buf).Encode(body)
		// post json data to elasticsearch address
		req, err := http.NewRequest("POST", url, buf)
		// set header to application json
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if nil != err {

			return res
		}
		// close response body
		defer resp.Body.Close()

		fmt.Println("response Status:", resp.Status)

	}
	res = true
	return res
}

// parse html
func parseHtml(url string) ([]product, int) {

	output := product{}
	counter := 0
	res := []product{}
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	// set get request
	request, err := http.NewRequest("Get", url, nil) // request here "GET is http type" "url is path" "nil is 0"
	if nil != err {
		fmt.Println("error requesting [[", url, "]] url", err)
		return res, counter

	}
	// set response header
	response, err := client.Do(request)
	if nil != err {
		//	panic(err.Error())
		fmt.Println("error setting  [[", url, "]] url header", err)
		return res, counter
	}
	defer response.Body.Close() // close response.body
	//defer client.CloseIdleConnections()

	doc, err := goquery.NewDocumentFromReader(response.Body) //parse html goquery
	if nil != err {
		//panic(err.Error())
		fmt.Println("error getting body of the response", err)
		return res, counter
	}

	fmt.Println("body", response.Body)

	// analayze html as default from search-result id
	doc.Find("#search-results").Find(".s-item-container").Each(func(index int, div *goquery.Selection) {
		// get title value
		title := div.Find("h2").Text()
		// get image url
		img_path, exists := div.Find(".a-fixed-left-grid-col.a-col-left").Find("img").Attr("src")
		fmt.Println("set path", img_path, "/", title)
		if exists {
			fmt.Println("title: ", title, "\n img path ", img_path)
			output.title = title
			output.imgPath = img_path
			res = append(res, output)
			counter++
		}
	})
	if counter == 0 {
		// check if upper code did not work use below code
		fmt.Println("category changed design", counter)
		doc.Find(".sg-col-inner").Find(".s-include-content-margin.s-border-bottom").Each(func(index int, div *goquery.Selection) {

			// get title value from h2 class
			title := div.Find("h2").Find(".a-size-medium.a-color-base.a-text-normal").Text()
			// get image url from s-image class
			img_path, exists := div.Find(".s-image").Attr("src")
			fmt.Println("set path", img_path, "/", title)
			if exists {
				fmt.Println("title: ", title, "\n img path ", img_path)
				output.title = title
				output.imgPath = img_path
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

	stat := false
	output := []product{}
	menuStat := []status{}
	list := selectMenu()
	total := 0
	if nil != list {
		start := time.Now()
		fmt.Println("time started ", start)
		for index := 0; index < len(list); index++ {
			fmt.Println("tb_menu list ", list[index])
			// parsing html document
			result, counter := parseHtml(list[index].URLPath)
			if 0 != counter {
				// if data exists append to temproray file
				fmt.Println("data exists ", counter)
				output = append(output, result...)
				// add products
				total = total + counter
				status := status{list[index].menuID, 1}
				menuStat = append(menuStat, status)

			} else {
				// if data does not exists set menu status to 2
				fmt.Println("no data in[title: ", list[index].title, "] [url:", list[index].URLPath, "] [", list[index].menuID, "]", counter)
				// set values to status struct
				status := status{list[index].menuID, 2}
				// add status to existing menusStat
				menuStat = append(menuStat, status)
			}

		}
		// set start time
		secStart := start.Unix()

		end := time.Now()
		secEnd := end.Unix()
		if 0 != total {
			// get time duration function exucution
			duration := secEnd - secStart
			fmt.Println("data collection took ", duration, "seconds")
			fmt.Println("total ", total)
			// save products to database
			insertProducts(output, total)
			// update menu status
			updateMenu(menuStat)

			stat = true
		} else {
			stat = false
		}

	} else {
		fmt.Println("error menut data is empty ", list)
		stat = false
	}

	return stat

}

func main() {

	result := htppUrlConnect() // result is bool variable value is true if data collected false if data is not collected
	fmt.Println("data collected ", result)
	if result {
		selectProducts()
	}
}
