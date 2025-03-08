package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"

	//binary实现数字与字节序列之间的简单转换，以及变长整数（varint）的编码和解码
	"encoding/binary"
)

type Data struct {
	TypeId  int    `json:"typeId"`
	Message string `json:"message"`
}

func Stream(name string, msg map[string]interface{}) *redis.XAddArgs {
	return &redis.XAddArgs{
		Stream: name,
		ID:     "*",
		Values: msg,
	}
}

func InitLog(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("无法进行记录", err)
	}
	return file
}

func Record(file *os.File, msg string) {
	//带缓存区写入
	writer := bufio.NewWriter(file)
	writer.WriteString(msg + "\r\n")
	//将缓冲区的内容写入文件
	writer.Flush()
}

// Encode 将消息编码
func Encode(message string) ([]byte, error) {
	// 读取消息的长度，转换成int32类型（占4个字节）将包体的长度用四个字节表示
	var length = int32(len(message))
	//bytes.Buffer提供一个动态长的字节切片（字节缓冲区），实现了 io.Reader、io.Writer、io.ByteReader、io.ByteWriter、io.RuneReader 和 io.StringWriter 等接口
	var pkg = new(bytes.Buffer)
	//Write处理固定长度值的编码和解码
	// 写入消息头（包头）
	err := binary.Write(pkg, binary.LittleEndian, length)
	if err != nil {
		//写入错误
		return nil, err
	}
	//Write(w io.Writer, order ByteOrder, data interface{}) error 将data序列化成字节流，并写入到io.Writer中,data必须是固定长度的数据值或固定长度数据的切片
	//LittleEndian表示小端字节序,BigEndian表示大端字节序
	// 写入消息实体（包体）
	err = binary.Write(pkg, binary.LittleEndian, []byte(message))
	if err != nil {
		return nil, err
	}
	//Bytes()获取缓冲区的字节切片副本
	return pkg.Bytes(), nil
}

// Decode 解码消息
func Decode(reader *bufio.Reader) (string, error) {
	// 读取消息的长度
	lengthByte, _ := reader.Peek(4) // 读取前4个字节的数据（获取到包体的长度）
	//将长度作为初始数据存入新的缓冲区
	lengthBuff := bytes.NewBuffer(lengthByte)
	//获取到前四个字节内容，转换为10进制保存
	var length int32
	//Read(r io.Reader, order ByteOrder, data interface{}) error 从io.Reader中读取字节流，并根据指定的字节序（ByteOrder）将字节流解码到data中
	err := binary.Read(lengthBuff, binary.LittleEndian, &length)
	if err != nil {
		return "", err
	}
	// Buffered返回缓冲中现有的可读取的字节数。length表示包体的字节数，4表示包头的字节数
	if int32(reader.Buffered()) < length+4 {
		return "", err
	}
	// 读取真正的消息数据
	pack := make([]byte, int(4+length))
	//包含的有4个字节的包头和剩下的包体
	_, err = reader.Read(pack)
	if err != nil {
		return "", err
	}
	return string(pack[4:]), err
}
