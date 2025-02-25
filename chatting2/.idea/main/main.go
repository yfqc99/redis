package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func chatting(conn net.Conn, user string) {
	filePath := "G:/goProject/src/chatting/document/record.txt"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("无法对对话进行记录")
	}
	defer file.Close()
	defer conn.Close()
	for {
		//存储客户端发送的数据
		buf := make([]byte, 1024)
		//读取到客户端发送过来的数据，并返回字节数
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(user, "已经下线")
			return
		}
		//带缓存区写入
		writer := bufio.NewWriter(file)
		str := user + ":" + string(buf[:n])
		writer.WriteString(str)
		//将缓冲区的内容写入文件
		writer.Flush()
		fmt.Print(str)
	}
}
func main() {
	//粘包
	//心跳检测
	fmt.Println("请输入网名进入聊天室")
	//监听连接
	listen, err := net.Listen("tcp", "0.0.0.0:8888")
	if err != nil {
		fmt.Println("网络连接错误")
		return
	} else {
		var user string
		for {
			//只有当有新的客户端发送连接请求的时候会创立连接，否则就会一直等待
			conn, err := listen.Accept()
		label:
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			user = strings.Trim(string(buf[:n]), " \r\n")
			//当网名为空时登录失败
			if err1 != nil || user == "" || err != nil {
				fmt.Println("登录失败,请重新登录")
				goto label
			} else {
				fmt.Println("-----------------欢迎" + user + "进入网络聊天室-----------------")
				go chatting(conn, user)
			}
		}
	}
}
