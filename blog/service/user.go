package service

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mszlu521/msgo/orm"
	"net/url"
)

//type User struct {
//	Id       int64  `msorm:"id,auto_increment"`
//	UserName string `msorm:"user_name"`
//	Password string `msorm:"password"`
//	Age      int    `msorm:"age"`
//}

type User struct {
	Id       int64
	UserName string
	Password string
	Age      int
}

func SaveUser() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	user := &User{
		UserName: "mszlu",
		Password: "123456",
		Age:      30,
	}
	id, _, err := db.New(&User{}).Insert(user)
	if err != nil {
		panic(err)
	}
	fmt.Println(id)

	db.Close()
}

func SaveUserBatch() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	user := &User{
		UserName: "mszlu222",
		Password: "12345612",
		Age:      54,
	}
	user1 := &User{
		UserName: "mszlu111",
		Password: "123456111",
		Age:      12,
	}
	var users []any
	users = append(users, user, user1)
	id, _, err := db.New(&User{}).InsertBatch(users)
	if err != nil {
		panic(err)
	}
	fmt.Println(id)

	db.Close()
}

func UpdateUser() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	//id, _, err := db.New().Where("id", 1006).Where("age", 54).Update(user)
	//单个插入
	user := &User{
		UserName: "mszlu",
		Password: "123456",
		Age:      30,
	}
	id, _, err := db.New(&User{}).Insert(user)
	if err != nil {
		panic(err)
	}
	fmt.Println(id)

	//批量插入
	var users []any
	users = append(users, user)
	id, _, err = db.New(&User{}).InsertBatch(users)
	if err != nil {
		panic(err)
	}
	fmt.Println(id)
	//更新
	id, _, err = db.
		New(&User{}).
		Where("id", 1006).
		UpdateParam("age", 100).
		Update()
	//查询单行数据
	err = db.New(&User{}).
		Where("id", 1006).
		Or().
		Where("age", 30).
		SelectOne(user, "user_name")
	//查询多行数据
	users, err = db.New(&User{}).Select(&User{})
	if err != nil {
		panic(err)
	}
	for _, v := range users {
		u := v.(*User)
		fmt.Println(u)
	}

	if err != nil {
		panic(err)
	}
	fmt.Println(id)

	db.Close()
}

func SelectOne() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	user := &User{}
	err := db.New(user).
		Where("id", 1006).
		Or().
		Where("age", 30).
		SelectOne(user, "user_name")
	if err != nil {
		panic(err)
	}
	fmt.Println(user)

	db.Close()
}

func Select() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	user := &User{}
	users, err := db.New(user).Where("id", 1000).Order("id", "asc", "age", "desc").Select(user)
	if err != nil {
		panic(err)
	}
	for _, v := range users {
		u := v.(*User)
		fmt.Println(u)
	}
	db.Close()
}

func Count() {
	dataSourceName := fmt.Sprintf("root:root@tcp(localhost:3306)/msgo?charset=utf8&loc=%s&parseTime=true", url.QueryEscape("Asia/Shanghai"))
	db := orm.Open("mysql", dataSourceName)
	db.Prefix = "msgo_"
	user := &User{}
	count, err := db.New(user).Count()
	if err != nil {
		panic(err)
	}
	fmt.Println(count)
	db.Close()
}
