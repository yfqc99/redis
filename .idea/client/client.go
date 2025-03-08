package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("网名:")
	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		fmt.Println("无法连接聊天室")
		return
	}
	//标准输入，获得终端输入的数据
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("已退出登录")
		}
		//向服务器发送数据
		_, err1 := conn.Write([]byte(line))
		if err1 != nil {
			fmt.Println("发送失败")
		}
	}
}
