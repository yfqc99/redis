package main

import (
	"bufio"
	"chatting2/utils"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type User struct {
	//id   uuid.UUID
	name string
	conn net.Conn
}

type userList struct {
	//string 状态码
	List map[string]User
	mu   sync.Mutex
}

var (
	userl userList
	msg   chan string
	wg    sync.WaitGroup
	ctx   = context.Background()
	rdb   = redis.NewClient(&redis.Options{
		Addr:     "192.168.157.129:6379",
		Password: "",
		DB:       0,
	})
)

func main() {
	defer rdb.Close()
	fmt.Println("等待连接")
	//监听连接
	listen, err := net.Listen("tcp", "0.0.0.0:8888")
	if err != nil {
		fmt.Println("网络连接错误", err)
		return
	}
	msg = make(chan string)
	userl.List = make(map[string]User)
	//提前清空键值对
	rdb.Del(ctx, "rank")
	for {
		//只有当有新的客户端发送连接请求的时候会创立连接，否则就会一直等待
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("接受连接失败:", err)
			continue
		}
		message, user := getUser(conn)
		wg.Add(2)
		go accept(conn, user)
		go send()
		msg <- message
	}
	wg.Wait()
}

// 排行
func rank() []string {
	//获得排行
	res, err := rdb.ZRevRange(ctx, "rank", 0, -1).Result()
	if err != nil {
		fmt.Println("获取排行失败", err)
	}
	//处理原始数组
	indexStr := make([]string, len(res))
	for i, r := range res {
		indexStr[i] = fmt.Sprintf("%d	%s", i+1, r)
	}
	return indexStr
}

// 得到客户端信息
func getUser(conn net.Conn) (string, User) {
label:
	//接收第一次发送的消息
	reader := bufio.NewReader(conn)
	str, _ := utils.Decode(reader)
	//获取用户名
	username := strings.Trim(str, " \r\n")
	//当网名为空时登录失败
	if username == "" || username == " " {
		str, _ := utils.Encode("登录失败,请重新登录")
		conn.Write(str)
		goto label
	}
	if _, ok := userl.List[username]; ok {
		str, _ := utils.Encode("用户名重复，请重新登录")
		conn.Write(str)
		goto label
	}
	user := User{
		//id:   uuid.New(),
		name: username,
		conn: conn,
	}
	// 存储登录的用户信息
	userl.mu.Lock()
	userl.List[user.name] = user
	userl.mu.Unlock()
	message := "-----------------欢迎" + username + "进入网络聊天室-----------------"
	time := time.Now().String()
	fmt.Println(time + " " + username + "登录")
	record(time + " " + username + "登录")
	//添加到有序集合序列
	m := redis.Z{
		Score:  0,
		Member: username,
	}
	//这里可以和上面处理重复问题合并一下
	rdb.ZAddNX(ctx, "rank", &m)
	return message, user
}

// 写入日志
func record(msg string) {
	filePath := "G:/goProject/src/chatting2/document/record.txt"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("无法进行记录", err)
	}
	//带缓存区写入
	writer := bufio.NewWriter(file)
	writer.WriteString(msg + "\r\n")
	//将缓冲区的内容写入文件
	writer.Flush()
	defer file.Close()
}

// 接收消息
func accept(conn net.Conn, user User) {
	defer conn.Close()
	for {
		//if _,err := conn.Read()
		//存储客户端发送的数据
		reader := bufio.NewReader(conn)
		//读取到客户端发送过来的数据
		str1, err := utils.Decode(reader)
		time := time.Now().String()
		//输入exit 退出
		if strings.Trim(str1, "\r\n") == "exit" || err != nil {
			fmt.Println(time + " " + user.name + "已经下线")
			userl.mu.Lock()
			delete(userl.List, user.name)
			userl.mu.Unlock()
			msg <- user.name + "已经下线"
			//将成员从有序集合中删除
			rdb.ZRem(ctx, "rank", user.name)
			record(time + " " + user.name + "已经下线")
			wg.Done()
			//当下线时给客户端发送同意的消息
			str, _ := utils.Encode("ok")
			conn.Write(str)
			break
		}
		if strings.Trim(str1, "\r\n") == "rank" {
			rank := rank()
			str := strings.Join(rank, "\n")
			str1, _ := utils.Encode(str)
			conn.Write(str1)
			continue
		}
		str := user.name + ":" + strings.Trim(str1, " \r\n")
		msg <- str
		//将该成员对应的分数加1
		rdb.ZIncrBy(ctx, "rank", float64(1), user.name)
		fmt.Println(time + " " + str)
		record(time + " " + str)
	}
}

// 广播
func send() {
	for {
		message := <-msg
		userl.mu.Lock()
		for name, user := range userl.List {
			if strings.Split(message, ":")[0] == name {
				continue
			}
			str, _ := utils.Encode(message)
			// 发送数据
			_, err := user.conn.Write(str)
			if err != nil {
				fmt.Println(name, "已下线", err)
				delete(userl.List, name)
			}
		}
		userl.mu.Unlock()
	}
	defer wg.Done()
}
