package main

import (
	"bufio"
	"chatting2/utils"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	wg sync.WaitGroup
)

func main() {
	fmt.Println("输入网名进入，exit退出")
	fmt.Println("网名:")
	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		fmt.Println("无法连接聊天室")
		return
	}
	wg.Add(2)
	go sending(conn)
	go accept(conn)
	wg.Wait()
}

// 向服务端发送消息
func sending(conn net.Conn) {
	//defer conn.Close()
	//标准输入，获得终端输入的数据
	reader := bufio.NewReader(os.Stdin)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("消息输入失败", err)
		}
		str, _ := utils.Encode(msg)
		//向服务器发送数据
		_, err1 := conn.Write(str)
		if strings.Trim(msg, "\r\n") == "exit" {
			fmt.Println("已退出登录，再见")
			wg.Done()
			break
		}
		if err1 != nil {
			fmt.Println("z发送失败", err1)
		}
	}
}

// 接收服务端消息
func accept(conn net.Conn) {
	// 接收到发送的断开连接消息
	for {
		//存储接收到的数据
		reader := bufio.NewReader(conn)
		//读取数据
		str1, _ := utils.Decode(reader)
		str := strings.Trim(str1, " \r\n")
		if str == "ok" {
			break
		}
		fmt.Println(str)
	}
	wg.Done()
}
