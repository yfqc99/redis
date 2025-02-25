package main

import (
	"bufio"
	"chatting2/utils"
	"fmt"
	"github.com/google/uuid"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type User struct {
	id   uuid.UUID
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
)

//存在问题：服务端和客户端异常掉线后，都会分别进入死循环
//心跳检测应该可以解决

func main() {
	fmt.Println("等待连接")
	//监听连接
	listen, err := net.Listen("tcp", "0.0.0.0:8888")
	if err != nil {
		fmt.Println("网络连接错误", err)
		return
	}
	msg = make(chan string)
	userl.List = make(map[string]User)
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
		id:   uuid.New(),
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
		//存储客户端发送的数据
		reader := bufio.NewReader(conn)
		//读取到客户端发送过来的数据
		str1, _ := utils.Decode(reader)
		time := time.Now().String()
		//输入exit 退出
		if strings.Trim(str1, "\r\n") == "exit" {
			fmt.Println(time + " " + user.name + "已经下线")
			userl.mu.Lock()
			delete(userl.List, user.name)
			userl.mu.Unlock()
			msg <- user.name + "已经下线"
			record(time + " " + user.name + "已经下线")
			wg.Done()
			//当下线时给客户端发送同意的消息
			str, _ := utils.Encode("ok")
			conn.Write(str)
			break
		}
		str := user.name + ":" + strings.Trim(str1, " \r\n")
		msg <- str
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
