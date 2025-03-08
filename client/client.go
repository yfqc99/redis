package main

import (
	"bufio"
	"chatting2/utils"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	wg    sync.WaitGroup
	login bool
)

func main() {
	fmt.Println("输入网名进入，exit退出")
	fmt.Println("网名:")

	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		fmt.Println("无法连接聊天室")
		return
	}
	defer conn.Close()

	wg.Add(2)
	go sendMessages(conn)
	go receiveMessages(conn)
	wg.Wait()
}

func sendMessages(conn net.Conn) {
	defer wg.Done()

	reader := bufio.NewReader(os.Stdin)
	var send utils.Data

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("消息输入失败", err)
			return
		}
		msg = strings.TrimSpace(msg)

		if msg == "" {
			fmt.Println("内容不能为空，请重新输入")
			continue
		}
		if msg == "exit" {
			send.TypeId = 0
			send.Message = "已下线"
			jsonByte, _ := json.Marshal(send)
			str, _ := utils.Encode(string(jsonByte))
			conn.Write(str)
			wg.Done()
			break
		}
		if msg == "rank" {
			send.TypeId = 1
			send.Message = "------排行榜------"
			jsonByte, _ := json.Marshal(send)
			str, _ := utils.Encode(string(jsonByte))
			conn.Write(str)
			continue
		}
		send.TypeId = 2
		send.Message = msg
		jsonByte, _ := json.Marshal(send)
		str, _ := utils.Encode(string(jsonByte))
		conn.Write(str)
	}
}

func receiveMessages(conn net.Conn) {
	defer wg.Done()

	var receive utils.Data

	for {
		reader := bufio.NewReader(conn)
		str1, err := utils.Decode(reader)
		if err != nil {
			fmt.Println("用户已退出登录")
			os.Exit(1)
		}
		str := strings.TrimSpace(str1)
		if err := json.Unmarshal([]byte(str), &receive); err != nil {
			continue
		}

		if receive.TypeId == 0 {
			fmt.Println(receive.Message)
			break
		}

		if receive.Message != "" {
			fmt.Println(receive.Message)
		}
	}
}
